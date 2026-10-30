//! Anonymous usage data, sent to PostHog EU.
//!
//! On by default; `telemetry.set` is the owner's switch, and `MICRODUCK_TELEMETRY=0` or
//! `DO_NOT_TRACK=1` in `updaterd`'s environment forces it off. Events carry a random robot id,
//! the board, versions and the CPU architecture — never an IP, a name, an account, a serial, logs
//! or media. `docs/robot/telemetry.md` owns that list; change both together.
//!
//! The same design as Reachy Mini's daemon: a queue in memory, flushed once a minute by a task,
//! and a state file holding the choice, the id and the last version seen.

use std::collections::VecDeque;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Error, proto};

/// Where the state lives when the server is not told otherwise. `updaterd` passes its
/// `state_dir`; this is that directory on a robot.
pub const DEFAULT_PATH: &str = "/var/lib/robot/updater/telemetry.json";

const POSTHOG_URL: &str = "https://eu.i.posthog.com/batch/";
/// Public, write-only project key.
// shortcut: empty until the microduck PostHog project exists, and nothing is sent while it is.
const POSTHOG_KEY: &str = "";

const FLUSH_INTERVAL: Duration = Duration::from_secs(60);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(24 * 3600);
const MAX_QUEUE: usize = 500;
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct State {
    enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    robot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_version: Option<String>,
}

struct Inner {
    state: State,
    queue: VecDeque<Value>,
}

pub struct Telemetry {
    path: PathBuf,
    /// The properties every event carries.
    envelope: Value,
    inner: Mutex<Inner>,
}

impl Telemetry {
    pub fn new(path: PathBuf) -> Self {
        Self::with_envelope(path, envelope())
    }

    fn with_envelope(path: PathBuf, envelope: Value) -> Self {
        let state = load(&path);
        Self {
            path,
            envelope,
            inner: Mutex::new(Inner {
                state,
                queue: VecDeque::new(),
            }),
        }
    }

    pub fn status(&self) -> proto::TelemetryResult {
        result(&self.lock().state)
    }

    /// Persist the owner's choice and apply it at once. Off forgets the robot id and the last
    /// version, so turning it back on starts an identity nothing links to the old one.
    pub fn set(&self, enabled: bool) -> Result<proto::TelemetryResult, Error> {
        let mut inner = self.lock();
        let next = if enabled {
            State {
                enabled,
                robot_id: match inner.state.robot_id.clone() {
                    Some(id) => Some(id),
                    None => Some(new_id()?),
                },
                ..inner.state.clone()
            }
        } else {
            inner.queue.clear();
            State::default()
        };
        self.persist(&next)?;
        inner.state = next;
        Ok(result(&inner.state))
    }

    /// Report this start, then flush the queue every minute and send a heartbeat once a day.
    pub async fn run(self: Arc<Self>) {
        self.started();
        let client = match crate::source::http::client() {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(error = %e, "telemetry off for this run");
                return;
            }
        };
        let since = tokio::time::Instant::now();
        let mut flush = tokio::time::interval(FLUSH_INTERVAL);
        let mut heartbeat =
            tokio::time::interval_at(since + HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                _ = flush.tick() => self.flush(&client).await,
                _ = heartbeat.tick() => self.capture(
                    "daily_heartbeat",
                    json!({ "uptime_s": since.elapsed().as_secs() }),
                ),
            }
        }
    }

    /// `daemon_started`, and `update_result` when the version moved since the last start.
    fn started(&self) {
        let previous = {
            let mut inner = self.lock();
            if !active(&inner.state) {
                return;
            }
            let mut next = inner.state.clone();
            if next.robot_id.is_none() {
                match new_id() {
                    Ok(id) => next.robot_id = Some(id),
                    Err(e) => {
                        tracing::warn!(error = %e, "telemetry off for this run");
                        return;
                    }
                }
            }
            let previous = next.last_version.replace(VERSION.to_owned());
            if let Err(e) = self.persist(&next) {
                tracing::warn!(error = %e, "telemetry off for this run");
                return;
            }
            inner.state = next;
            previous
        };
        self.capture("daemon_started", json!({}));
        if let Some(from) = previous.filter(|v| v != VERSION) {
            self.capture(
                "update_result",
                json!({ "from_version": from, "to_version": VERSION, "success": true }),
            );
        }
    }

    fn capture(&self, event: &str, properties: Value) {
        let mut inner = self.lock();
        let Some(id) = inner
            .state
            .robot_id
            .clone()
            .filter(|_| active(&inner.state))
        else {
            return;
        };
        let mut merged = self.envelope.clone();
        if let (Some(all), Value::Object(extra)) = (merged.as_object_mut(), properties) {
            all.extend(extra);
        }
        if inner.queue.len() >= MAX_QUEUE {
            inner.queue.pop_front();
        }
        // No `timestamp`: the board has no RTC, so before NTP its clock is wrong, and PostHog
        // stamps an event with the time it arrives instead. A queued event is late by its wait.
        inner
            .queue
            .push_back(json!({ "event": event, "distinct_id": id, "properties": merged }));
    }

    async fn flush(&self, client: &reqwest::Client) {
        let batch: Vec<Value> = self.lock().queue.drain(..).collect();
        if batch.is_empty() {
            return;
        }
        let sent = client
            .post(POSTHOG_URL)
            .timeout(Duration::from_secs(10))
            .json(&json!({ "api_key": POSTHOG_KEY, "batch": batch }))
            .send()
            .await;
        let retry = match sent {
            Ok(response) if response.status().is_success() => false,
            Ok(response) => {
                let status = response.status();
                let retry = status.as_u16() == 429 || status.is_server_error();
                if !retry {
                    tracing::warn!(%status, "telemetry rejected");
                }
                retry
            }
            // Offline, or before NTP, when TLS cannot validate anything.
            Err(e) => {
                tracing::debug!(error = %e, "telemetry not sent");
                true
            }
        };
        if retry {
            let mut inner = self.lock();
            // Dropped if the owner opted out, or got a new id, while it was in flight.
            if batch[0]["distinct_id"].as_str() == inner.state.robot_id.as_deref() {
                for event in batch.into_iter().rev() {
                    inner.queue.push_front(event);
                }
                inner.queue.truncate(MAX_QUEUE);
            }
        }
    }

    fn persist(&self, state: &State) -> Result<(), Error> {
        let bytes = serde_json::to_vec(state).expect("State serialises");
        crate::fsutil::write_atomic(&self.path, &bytes)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The stored choice. No file is the default, on; a file that cannot be read is off, so a
/// corrupted choice never turns into consent.
fn load(path: &Path) -> State {
    match std::fs::read(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => State {
            enabled: true,
            ..State::default()
        },
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            tracing::warn!(path = %path.display(), error = %e, "unreadable telemetry state; off");
            State::default()
        }),
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "unreadable telemetry state; off");
            State::default()
        }
    }
}

fn result(state: &State) -> proto::TelemetryResult {
    proto::TelemetryResult {
        enabled: state.enabled,
        active: active(state),
    }
}

fn active(state: &State) -> bool {
    state.enabled && !POSTHOG_KEY.is_empty() && !blocked_by_env()
}

fn blocked_by_env() -> bool {
    let var = |k| {
        std::env::var(k)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    };
    matches!(
        var("MICRODUCK_TELEMETRY").as_str(),
        "0" | "false" | "no" | "off"
    ) || matches!(var("DO_NOT_TRACK").as_str(), "1" | "true" | "yes" | "on")
}

/// 128 random bits, in hex. Random rather than derived from the SoC serial, so it identifies a
/// robot to nobody but this file.
fn new_id() -> Result<String, Error> {
    let path = Path::new("/dev/urandom");
    let mut bytes = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut bytes))
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn envelope() -> Value {
    json!({
        "daemon_version": VERSION,
        "os_version": os_version(),
        "board": robotd_params::board::Board::detected().map_or("unknown", |b| b.label()),
        "arch": std::env::consts::ARCH,
        "$geoip_disable": true,
        "$process_person_profile": false,
    })
}

/// `ID VERSION_ID` from os-release: the image version on a microduck_os board.
fn os_version() -> String {
    let release = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    let field = |key: &str| {
        release
            .lines()
            .find_map(|l| l.strip_prefix(key)?.strip_prefix('='))
            .map(|v| v.trim_matches('"').to_owned())
            .unwrap_or_default()
    };
    format!("{} {}", field("ID"), field("VERSION_ID"))
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(dir: &tempfile::TempDir) -> Telemetry {
        Telemetry::with_envelope(dir.path().join("telemetry.json"), json!({}))
    }

    #[test]
    fn on_by_default_and_off_forgets_the_id() {
        let dir = tempfile::tempdir().unwrap();
        assert!(at(&dir).status().enabled, "no file means the default, on");

        let t = at(&dir);
        t.set(true).unwrap();
        let first = t.lock().state.robot_id.clone().unwrap();
        assert_eq!(first.len(), 32);

        t.set(false).unwrap();
        assert!(!at(&dir).status().enabled, "off survives a restart");
        assert_eq!(at(&dir).lock().state, State::default(), "and keeps nothing");

        let t = at(&dir);
        t.set(true).unwrap();
        assert_ne!(t.lock().state.robot_id.clone().unwrap(), first);
    }

    #[test]
    fn an_unreadable_file_is_off() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("telemetry.json"), b"{not json").unwrap();
        assert!(!at(&dir).status().enabled);
    }

    #[test]
    fn nothing_is_queued_without_a_key() {
        let dir = tempfile::tempdir().unwrap();
        let t = at(&dir);
        t.set(true).unwrap();
        t.capture("daemon_started", json!({}));
        assert_eq!(t.lock().queue.len(), usize::from(!POSTHOG_KEY.is_empty()));
        assert!(!t.status().active || !POSTHOG_KEY.is_empty());
    }
}
