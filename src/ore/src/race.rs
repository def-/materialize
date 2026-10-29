// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Race points: named code locations at which an external driver can record
//! and force the interleaving of concurrent actors.
//!
//! Race points are inert unless the process starts with `MZ_RACE_DIR` set.
//! The driver (`bin/race-viz`) then coordinates all processes through files in
//! that directory, which is what lets an ordering span environmentd and
//! clusterd:
//!
//! * `control` names the active run and its ordering constraints. Every
//!   process re-reads it at most every [`CONTROL_POLL_INTERVAL`], on a point
//!   hit. Without an active run, points record and enforce nothing.
//! * `runs/<run>/trace-<pid>.jsonl` receives one JSON object per event. Each
//!   line is written with its own `write` call, so a trace survives a panic or
//!   abort of the process.
//! * `runs/<run>/flags/<n>` exists once constraint `n` is satisfied.
//!
//! A point hit is identified by its actor, its point name, and how many times
//! that actor hit that point in the current run. Neither addresses nor IDs are
//! stable across runs, but this triple is, as long as the test is
//! deterministic up to the interleavings being explored.
//!
//! The actor is the innermost [`scope`] (set for every task spawned through
//! [`crate::task`]), possibly renamed by [`set_actor`], else the thread name.
//!
//! A constraint `A before B` makes the hit `B` wait until hit `A` fired. The
//! `before` side fires either when the point is reached (`pre`), or when the
//! [`Guard`] returned by the point drops (`post`). Binding the guard
//! (`let _race = race_point!(..)`) extends `post` across the action that
//! follows the point. A waiting hit gives up after the run's timeout and
//! records a `timeout` event, which is how the driver detects an ordering that
//! cannot happen.

use std::cell::RefCell;
use std::fmt::{Display, Write as _};
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::collections::HashMap;

/// How often a process re-reads the control file at most.
pub const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// How often a waiting point checks its flag.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(1);

static STATE: LazyLock<Option<State>> = LazyLock::new(|| {
    let dir = PathBuf::from(std::env::var_os("MZ_RACE_DIR")?);
    let process = std::env::args()
        .next()
        .as_deref()
        .and_then(|arg0| Path::new(arg0).file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".into());
    Some(State {
        dir,
        process,
        inner: Mutex::new(Inner {
            last_poll: None,
            control_version: None,
            run: None,
        }),
    })
});

tokio::task_local! {
    static ACTOR: RefCell<String>;
}

/// Reports whether race points are active in this process.
#[inline]
pub fn enabled() -> bool {
    STATE.is_some()
}

/// Runs `future` with `actor` as the actor of its race points.
pub async fn scope<F: Future>(actor: String, future: F) -> F::Output {
    ACTOR.scope(RefCell::new(actor), future).await
}

/// Wraps a task's future in a [`scope`] named after the task, if race points
/// are enabled.
pub(crate) fn scope_task<T: 'static>(
    name: impl FnOnce() -> String,
    future: Pin<Box<dyn Future<Output = T> + Send>>,
) -> Pin<Box<dyn Future<Output = T> + Send>> {
    if enabled() {
        Box::pin(scope(name(), future))
    } else {
        future
    }
}

/// Renames the actor of the innermost [`scope`].
///
/// Has no effect outside a scope. Lets a long-lived task take a name that a
/// test can predict, for example a pgwire connection that adopts its
/// `application_name`.
pub fn set_actor(actor: impl Into<String>) {
    if enabled() {
        let actor = actor.into();
        let _ = ACTOR.try_with(|a| *a.borrow_mut() = actor);
    }
}

fn current_actor() -> String {
    ACTOR
        .try_with(|a| a.borrow().clone())
        .ok()
        .or_else(|| std::thread::current().name().map(str::to_owned))
        .unwrap_or_else(|| "unnamed-thread".into())
}

/// What a race point does to the state its key names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Observes the state.
    Read,
    /// Changes the state.
    Write,
    /// Destroys the state.
    Drop,
}

impl Access {
    fn as_str(self) -> &'static str {
        match self {
            Access::Read => "read",
            Access::Write => "write",
            Access::Drop => "drop",
        }
    }
}

/// A race point about to be hit. Built by [`race_point!`] and
/// [`race_point_async!`].
#[derive(Debug)]
pub struct Point {
    name: &'static str,
    file: &'static str,
    line: u32,
    key: Option<String>,
    access: Option<Access>,
    note: Option<String>,
}

impl Point {
    #[doc(hidden)]
    pub fn new(name: &'static str, file: &'static str, line: u32) -> Self {
        Point {
            name,
            file,
            line,
            key: None,
            access: None,
            note: None,
        }
    }

    /// Names the shared state this point touches. Hits on the same key by
    /// different actors, at least one of them not a read, are the
    /// communication points the viewer highlights.
    pub fn key(mut self, key: impl Display) -> Self {
        self.key = Some(key.to_string());
        self
    }

    /// Sets how this point touches its key.
    pub fn kind(mut self, access: Access) -> Self {
        self.access = Some(access);
        self
    }

    /// Attaches free-form context shown in the viewer.
    pub fn note(mut self, note: impl Display) -> Self {
        self.note = Some(note.to_string());
        self
    }

    /// Hits the point, blocking the thread while a constraint holds it.
    ///
    /// NOTE: Blocking a tokio worker stalls every task on it. Prefer
    /// [`Point::hit`] in async code.
    pub fn hit_blocking(self) -> Option<Guard> {
        let (hit, waits) = self.arrive()?;
        let deadline = Instant::now() + hit.timeout;
        for wait in &waits {
            loop {
                if wait.flag.exists() {
                    hit.record("resume", Some(wait.constraint));
                    break;
                }
                if Instant::now() >= deadline {
                    hit.record("timeout", Some(wait.constraint));
                    break;
                }
                std::thread::sleep(WAIT_POLL_INTERVAL);
            }
        }
        Some(hit.depart())
    }

    /// Hits the point, yielding while a constraint holds it.
    pub async fn hit(self) -> Option<Guard> {
        let (hit, waits) = self.arrive()?;
        let deadline = Instant::now() + hit.timeout;
        for wait in &waits {
            loop {
                if wait.flag.exists() {
                    hit.record("resume", Some(wait.constraint));
                    break;
                }
                if Instant::now() >= deadline {
                    hit.record("timeout", Some(wait.constraint));
                    break;
                }
                tokio::time::sleep(WAIT_POLL_INTERVAL).await;
            }
        }
        Some(hit.depart())
    }

    /// Counts the hit, records it, fires `pre` wakes, and returns the flags
    /// the hit has to wait for. Returns `None` without an active run.
    fn arrive(self) -> Option<(Hit, Vec<Wait>)> {
        let state = STATE.as_ref()?;
        let actor = current_actor();
        let span = tracing::Span::current()
            .metadata()
            .map(|m| m.name())
            .unwrap_or("");

        let mut pre_wakes = Vec::new();
        let mut post_wakes = Vec::new();
        let mut waits = Vec::new();
        let mut hit = {
            let mut inner = state.inner.lock().expect("lock poisoned");
            inner.refresh(state);
            let run = inner.run.as_mut()?;
            let counter = run.hits.entry((actor.clone(), self.name)).or_insert(0);
            *counter += 1;
            let hit = Hit {
                point: self,
                actor,
                hit_no: *counter,
                span,
                run_id: run.id.clone(),
                timeout: run.timeout,
                post_wakes: Vec::new(),
            };
            for (i, c) in run.constraints.iter().enumerate() {
                if c.before.matches(&hit) {
                    let wakes = if c.post {
                        &mut post_wakes
                    } else {
                        &mut pre_wakes
                    };
                    wakes.push((i, run.flag(i)));
                }
                if c.after.matches(&hit) {
                    waits.push(Wait {
                        constraint: i,
                        flag: run.flag(i),
                    });
                }
            }
            hit
        };
        hit.post_wakes = post_wakes;

        hit.record("hit", None);
        for (constraint, flag) in &pre_wakes {
            fire(&hit, *constraint, flag);
        }
        for wait in &waits {
            hit.record("wait", Some(wait.constraint));
        }
        Some((hit, waits))
    }
}

fn fire(hit: &Hit, constraint: usize, flag: &Path) {
    if let Err(e) = File::create(flag) {
        tracing::warn!("race point cannot create flag {}: {e}", flag.display());
    }
    hit.record("wake", Some(constraint));
}

struct Wait {
    constraint: usize,
    flag: PathBuf,
}

struct Hit {
    point: Point,
    actor: String,
    hit_no: u64,
    span: &'static str,
    run_id: String,
    timeout: Duration,
    post_wakes: Vec<(usize, PathBuf)>,
}

impl Hit {
    fn depart(self) -> Guard {
        Guard { hit: self }
    }

    fn record(&self, event: &str, constraint: Option<usize>) {
        let Some(state) = STATE.as_ref() else { return };
        let ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut line = String::with_capacity(256);
        line.push('{');
        let _ = write!(line, "\"ts_ns\":{ts_ns},\"pid\":{}", std::process::id());
        json_field(&mut line, "process", &state.process);
        json_field(&mut line, "actor", &self.actor);
        json_field(&mut line, "point", self.point.name);
        let _ = write!(line, ",\"hit\":{}", self.hit_no);
        json_field(&mut line, "event", event);
        if let Some(c) = constraint {
            let _ = write!(line, ",\"constraint\":{c}");
        }
        if let Some(key) = &self.point.key {
            json_field(&mut line, "key", key);
        }
        if let Some(access) = self.point.access {
            json_field(&mut line, "access", access.as_str());
        }
        if let Some(note) = &self.point.note {
            json_field(&mut line, "note", note);
        }
        json_field(&mut line, "span", self.span);
        json_field(&mut line, "file", self.point.file);
        let _ = write!(line, ",\"line\":{}", self.point.line);
        line.push_str("}\n");

        let mut inner = state.inner.lock().expect("lock poisoned");
        // The control file may have moved on to another run while this hit
        // waited. Its events belong to the run it started in.
        if let Some(run) = inner.run.as_mut().filter(|run| run.id == self.run_id) {
            let _ = run.trace.write_all(line.as_bytes());
        }
    }
}

/// Fires the `post` wakes of a race point hit when dropped.
#[derive(Debug)]
pub struct Guard {
    hit: Hit,
}

impl std::fmt::Debug for Hit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hit")
            .field("actor", &self.actor)
            .field("point", &self.point.name)
            .field("hit", &self.hit_no)
            .finish_non_exhaustive()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        for (constraint, flag) in &self.hit.post_wakes {
            fire(&self.hit, *constraint, flag);
        }
    }
}

fn json_field(out: &mut String, name: &str, value: &str) {
    let _ = write!(out, ",\"{name}\":\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c < ' ' => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

struct State {
    dir: PathBuf,
    process: String,
    inner: Mutex<Inner>,
}

struct Inner {
    last_poll: Option<Instant>,
    /// Modification time and length of the control file when last read.
    control_version: Option<(SystemTime, u64)>,
    run: Option<Run>,
}

impl Inner {
    fn refresh(&mut self, state: &State) {
        let now = Instant::now();
        if self
            .last_poll
            .is_some_and(|last| now.duration_since(last) < CONTROL_POLL_INTERVAL)
        {
            return;
        }
        self.last_poll = Some(now);

        let path = state.dir.join("control");
        let version = fs::metadata(&path)
            .ok()
            .and_then(|m| Some((m.modified().ok()?, m.len())));
        if version == self.control_version {
            return;
        }
        self.control_version = version;
        let control = fs::read_to_string(&path).unwrap_or_default();
        self.run = match Control::parse(&control) {
            Ok(Some(control)) => Run::open(state, control),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!("ignoring invalid race control file {}: {e}", path.display());
                None
            }
        };
    }
}

struct Run {
    id: String,
    dir: PathBuf,
    timeout: Duration,
    constraints: Vec<Constraint>,
    hits: HashMap<(String, &'static str), u64>,
    trace: File,
}

impl Run {
    fn open(state: &State, control: Control) -> Option<Run> {
        let dir = state.dir.join("runs").join(&control.run);
        let open = || {
            fs::create_dir_all(dir.join("flags"))?;
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(format!("trace-{}.jsonl", std::process::id())))
        };
        match open() {
            Ok(trace) => Some(Run {
                id: control.run,
                dir,
                timeout: control.timeout,
                constraints: control.constraints,
                hits: HashMap::new(),
                trace,
            }),
            Err(e) => {
                tracing::warn!("cannot open race run directory {}: {e}", dir.display());
                None
            }
        }
    }

    fn flag(&self, constraint: usize) -> PathBuf {
        self.dir.join("flags").join(constraint.to_string())
    }
}

/// One hit of one point by one actor.
#[derive(Debug, PartialEq)]
struct HitId {
    actor: String,
    point: String,
    hit: u64,
}

impl HitId {
    fn matches(&self, hit: &Hit) -> bool {
        self.hit == hit.hit_no && self.point == hit.point.name && self.actor == hit.actor
    }
}

#[derive(Debug, PartialEq)]
struct Constraint {
    before: HitId,
    post: bool,
    after: HitId,
}

#[derive(Debug, PartialEq)]
struct Control {
    run: String,
    timeout: Duration,
    constraints: Vec<Constraint>,
}

impl Control {
    /// Parses the control file: tab-separated lines of
    ///
    /// ```text
    /// run         <run id>
    /// timeout_ms  <ms>
    /// order       <actor> <point> <hit> <pre|post> <actor> <point> <hit>
    /// ```
    ///
    /// Constraints are numbered by their order in the file. A file without a
    /// `run` line means no run is active.
    fn parse(s: &str) -> Result<Option<Control>, String> {
        let mut run = None;
        let mut timeout = Duration::from_secs(10);
        let mut constraints = Vec::new();
        for line in s.lines().filter(|l| !l.trim().is_empty()) {
            let fields: Vec<_> = line.split('\t').collect();
            let hit = |actor: &str, point: &str, hit: &str| -> Result<HitId, String> {
                Ok(HitId {
                    actor: actor.into(),
                    point: point.into(),
                    hit: hit.parse().map_err(|e| format!("bad hit {hit:?}: {e}"))?,
                })
            };
            match fields.as_slice() {
                ["run", id] => {
                    if id.is_empty() || id.contains(['/', '.']) {
                        return Err(format!("bad run id {id:?}"));
                    }
                    run = Some(id.to_string());
                }
                ["timeout_ms", ms] => {
                    let ms = ms.parse().map_err(|e| format!("bad timeout {ms:?}: {e}"))?;
                    timeout = Duration::from_millis(ms);
                }
                [
                    "order",
                    a_actor,
                    a_point,
                    a_hit,
                    mode,
                    b_actor,
                    b_point,
                    b_hit,
                ] => {
                    let post = match *mode {
                        "pre" => false,
                        "post" => true,
                        _ => return Err(format!("bad mode {mode:?}")),
                    };
                    constraints.push(Constraint {
                        before: hit(a_actor, a_point, a_hit)?,
                        post,
                        after: hit(b_actor, b_point, b_hit)?,
                    });
                }
                _ => return Err(format!("bad line {line:?}")),
            }
        }
        Ok(run.map(|run| Control {
            run,
            timeout,
            constraints,
        }))
    }
}

/// Hits a race point, blocking the thread while an ordering constraint holds
/// it. Evaluates to an `Option<`[`Guard`]`>`.
///
/// ```ignore
/// race_point!("mv_sealed_check", key = gid, kind = Access::Read, note = sealed);
/// ```
///
/// See the [module docs](crate::race) for the protocol. The arguments are only
/// evaluated while race points are enabled.
#[macro_export]
macro_rules! race_point {
    ($name:expr $(, $field:ident = $value:expr)* $(,)?) => {{
        if $crate::race::enabled() {
            $crate::race::Point::new($name, file!(), line!())
                $(.$field($value))*
                .hit_blocking()
        } else {
            None
        }
    }};
}

/// Like [`race_point!`], but yields instead of blocking. Must be awaited.
#[macro_export]
macro_rules! race_point_async {
    ($name:expr $(, $field:ident = $value:expr)* $(,)?) => {
        async {
            if $crate::race::enabled() {
                $crate::race::Point::new($name, file!(), line!())
                    $(.$field($value))*
                    .hit()
                    .await
            } else {
                None
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[crate::test]
    fn parse_control() {
        let control = Control::parse(
            "run\tr1\ntimeout_ms\t500\n\
             order\tcoordinator\tcheck\t1\tpost\tcompute-instance-u1\tseal\t2\n",
        )
        .expect("valid");
        assert_eq!(
            control,
            Some(Control {
                run: "r1".into(),
                timeout: Duration::from_millis(500),
                constraints: vec![Constraint {
                    before: HitId {
                        actor: "coordinator".into(),
                        point: "check".into(),
                        hit: 1,
                    },
                    post: true,
                    after: HitId {
                        actor: "compute-instance-u1".into(),
                        point: "seal".into(),
                        hit: 2,
                    },
                }],
            })
        );
        assert_eq!(Control::parse(""), Ok(None));
        assert!(Control::parse("run\t../x").is_err());
        assert!(Control::parse("order\ta\tb\tx\tpre\tc\td\t1").is_err());
    }

    /// Forces both orders of two threads that race to push to a vector.
    #[crate::test]
    #[cfg_attr(miri, ignore)] // file system access
    fn constraints_force_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY: no other test in this binary reads the environment
        // concurrently, and this is the only test that initializes `STATE`.
        unsafe { std::env::set_var("MZ_RACE_DIR", dir.path()) };
        assert!(enabled());

        let race = |run: &str, order: &str, first_delay: u64| {
            fs::write(
                dir.path().join("control"),
                format!("run\t{run}\ntimeout_ms\t5000\n{order}"),
            )
            .expect("write control");
            let log = std::sync::Arc::new(Mutex::new(Vec::new()));
            let spawn = |name: &'static str, delay: u64| {
                let log = std::sync::Arc::clone(&log);
                std::thread::Builder::new()
                    .name(name.into())
                    .spawn(move || {
                        std::thread::sleep(Duration::from_millis(delay));
                        let _race = race_point!("push", key = "log", kind = Access::Write);
                        log.lock().expect("lock").push(name);
                    })
                    .expect("spawn")
            };
            let a = spawn("a", first_delay);
            let b = spawn("b", 0);
            a.join().expect("a");
            b.join().expect("b");
            // Let the next run's control file differ in modification time.
            std::thread::sleep(CONTROL_POLL_INTERVAL * 2);
            let log = log.lock().expect("lock").clone();
            log
        };

        // Without a constraint, `b` wins because `a` sleeps first.
        assert_eq!(race("r1", "", 200), ["b", "a"]);
        // `b` waits until `a` left its point, which is after `a` pushed.
        let order = "order\ta\tpush\t1\tpost\tb\tpush\t1\n";
        assert_eq!(race("r2", order, 200), ["a", "b"]);

        let trace = fs::read_dir(dir.path().join("runs").join("r2"))
            .expect("run dir")
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with("trace-"))
            .map(|e| fs::read_to_string(e.path()).expect("trace"))
            .expect("trace file");
        for event in [
            "\"event\":\"wait\"",
            "\"event\":\"wake\"",
            "\"event\":\"resume\"",
        ] {
            assert!(trace.contains(event), "{event} missing from {trace}");
        }
    }

    #[crate::test]
    fn json_escaping() {
        let mut s = String::new();
        json_field(&mut s, "k", "a\"b\\c\nd\u{1}");
        assert_eq!(s, r#","k":"a\"b\\c\nd\u0001""#);
    }
}
