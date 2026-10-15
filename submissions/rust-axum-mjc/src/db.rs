use std::{
    error::Error,
    fmt,
    path::Path,
    sync::mpsc as std_mpsc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use rusqlite::{Connection, OpenFlags, PrepFlags, params};
use tokio::sync::{mpsc, oneshot};

use crate::model::Post;

const POST_QUERY: &str = "SELECT p.id, p.body, p.created_at, u.username,
    (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
    FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?1";
const FEED_QUERY: &str = "SELECT p.id, p.body, p.created_at, u.username,
    (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
    FROM posts p JOIN users u ON u.id = p.user_id
    ORDER BY p.created_at DESC, p.id DESC LIMIT 20";
#[cfg(test)]
const FEED_REFERENCE_QUERY: &str = "WITH feed AS (
    SELECT p.id, p.body, p.created_at, u.username
    FROM posts p JOIN users u ON u.id = p.user_id
    ORDER BY p.created_at DESC, p.id DESC
    LIMIT 20
)
SELECT f.id, f.body, f.created_at, f.username,
    (SELECT count(*) FROM likes l WHERE l.post_id = f.id)
FROM feed f
ORDER BY f.created_at DESC, f.id DESC";
const CREATE_POST_QUERY: &str =
    "INSERT INTO posts (user_id, body) VALUES (?1, ?2) RETURNING id, created_at";
const POST_EXISTS_QUERY: &str = "SELECT EXISTS (SELECT 1 FROM posts WHERE id = ?1)";
const INSERT_LIKE_QUERY: &str = "INSERT INTO likes (user_id, post_id) VALUES (?1, ?2) ON CONFLICT (user_id, post_id) DO NOTHING";
pub const DEFAULT_PAGE_CACHE_KIB: u64 = 256 * 1024;
const CHECKPOINT_IDLE_INTERVAL: Duration = Duration::from_secs(1);
const REQUEST_CAPACITY: usize = 4096;
// Match the benchmark p99 ceiling; admission and execution both count toward latency.
const REQUEST_ADMISSION_TIMEOUT: Duration = Duration::from_secs(1);
const READ_BATCH_LIMIT: usize = 1024;
const WRITE_BATCH_LIMIT: usize = 1024;
const BATCH_COLLECTION_WINDOW: Duration = Duration::from_millis(1);

fn sqlite_timings_enabled() -> bool {
    std::env::var("SQLITE_TIMINGS").is_ok_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

#[derive(Default)]
struct AdmissionTimings {
    completed: AtomicU64,
    read_timeouts: AtomicU64,
    write_timeouts: AtomicU64,
    wait_us: AtomicU64,
    max_wait_us: AtomicU64,
    last_report_ms: AtomicU64,
}

impl AdmissionTimings {
    fn record(&self, started: Instant, write: bool, timed_out: bool, available_slots: usize) {
        let wait_us = started.elapsed().as_micros() as u64;
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.wait_us.fetch_add(wait_us, Ordering::Relaxed);
        self.max_wait_us.fetch_max(wait_us, Ordering::Relaxed);
        if timed_out {
            let timeouts = if write {
                &self.write_timeouts
            } else {
                &self.read_timeouts
            };
            timeouts.fetch_add(1, Ordering::Relaxed);
        }
        let now_ms = unix_ms();
        let last_report = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_report) >= 1_000
            && self
                .last_report_ms
                .compare_exchange(last_report, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.report(false, Some(available_slots));
        }
    }

    fn report(&self, final_report: bool, available_slots: Option<usize>) {
        // Cumulative counters; producers still report when the worker is blocked.
        eprintln!(
            "SQLITE_TIMING {}",
            serde_json::json!({
                "event": "queue_admission", "end_unix_ms": unix_ms(), "final": final_report,
                "completed": self.completed.load(Ordering::Relaxed),
                "read_timeouts": self.read_timeouts.load(Ordering::Relaxed),
                "write_timeouts": self.write_timeouts.load(Ordering::Relaxed),
                "wait_us": self.wait_us.load(Ordering::Relaxed),
                "max_wait_us": self.max_wait_us.load(Ordering::Relaxed),
                "available_slots": available_slots,
            })
        );
    }
}

impl Drop for AdmissionTimings {
    fn drop(&mut self) {
        if self.completed.load(Ordering::Relaxed) != 0 {
            self.report(true, None);
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[derive(Default, serde::Serialize)]
struct BatchTotals {
    batches: u64,
    requests: usize,
    max_requests: usize,
    collection_us: u128,
    execution_us: u128,
    max_execution_us: u128,
}

struct WorkerTimings {
    last_report: Instant,
    read: BatchTotals,
    write: BatchTotals,
}

impl WorkerTimings {
    fn new() -> Self {
        Self {
            last_report: Instant::now(),
            read: BatchTotals::default(),
            write: BatchTotals::default(),
        }
    }

    fn record(
        &mut self,
        write: bool,
        requests: usize,
        started: Instant,
        collected: Instant,
        queued: usize,
    ) {
        let collection_us = collected.duration_since(started).as_micros();
        let execution_us = collected.elapsed().as_micros();
        let totals = if write {
            &mut self.write
        } else {
            &mut self.read
        };
        totals.batches += 1;
        totals.requests += requests;
        totals.max_requests = totals.max_requests.max(requests);
        totals.collection_us += collection_us;
        totals.execution_us += execution_us;
        totals.max_execution_us = totals.max_execution_us.max(execution_us);
        if execution_us >= 10_000 {
            let end_ms = unix_ms();
            eprintln!(
                "SQLITE_TIMING {}",
                serde_json::json!({
                    "event": "slow_batch", "kind": if write { "write" } else { "read" },
                    "requests": requests, "collection_us": collection_us, "execution_us": execution_us,
                    "start_unix_ms": end_ms.saturating_sub(((collection_us + execution_us) / 1_000) as u64),
                    "end_unix_ms": end_ms, "queued_after": queued,
                })
            );
        }
        if self.last_report.elapsed() >= Duration::from_secs(1) {
            self.report(false, Some(queued));
            self.read = BatchTotals::default();
            self.write = BatchTotals::default();
            self.last_report = Instant::now();
        }
    }

    fn report(&self, final_report: bool, queued: Option<usize>) {
        eprintln!(
            "SQLITE_TIMING {}",
            serde_json::json!({
                "event": "worker_batches", "end_unix_ms": unix_ms(), "final": final_report,
                "interval_us": self.last_report.elapsed().as_micros(),
                "read": self.read, "write": self.write, "queued_after": queued,
            })
        );
    }
}

impl Drop for WorkerTimings {
    fn drop(&mut self) {
        if self.read.batches != 0 || self.write.batches != 0 {
            self.report(true, None);
        }
    }
}

fn log_slow_commit(enabled: bool, started: Option<Instant>, requests: usize) {
    let Some(started) = started.filter(|_| enabled) else {
        return;
    };
    let commit_us = started.elapsed().as_micros();
    if commit_us < 1_000 {
        return;
    }
    let end_unix_ms = unix_ms();
    eprintln!(
        "SQLITE_TIMING {}",
        serde_json::json!({
            "event": "worker_commit",
            "requests": requests,
            "start_unix_ms": end_unix_ms.saturating_sub((commit_us / 1_000) as u64),
            "end_unix_ms": end_unix_ms,
            "commit_us": commit_us,
            "timestamp_resolution_ms": 1,
            "autocheckpoint_pages": 0,
        })
    );
}

// Odd generations mean SQL work is active. An unchanged even generation over
// two samples means no batch ran during that interval.
struct WorkerActivity<'a>(&'a AtomicU64);

impl Drop for WorkerActivity<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

fn checkpoint_worker(
    connection: Connection,
    stop: std_mpsc::Receiver<()>,
    activity: Arc<AtomicU64>,
    timings_enabled: bool,
) {
    let mut previous = None;
    let mut caught_up_at = None;
    loop {
        match stop.recv_timeout(CHECKPOINT_IDLE_INTERVAL) {
            Ok(()) | Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
        }
        let generation = activity.load(Ordering::Acquire);
        let unchanged = previous.replace(generation) == Some(generation);
        if generation & 1 != 0 || !unchanged || caught_up_at == Some(generation) {
            continue;
        }

        if timings_enabled {
            eprintln!(
                "SQLITE_TIMING {}",
                serde_json::json!({
                    "event": "checkpoint_start", "kind": "idle", "mode": "PASSIVE",
                    "start_unix_ms": unix_ms(), "activity_generation": generation,
                })
            );
        }
        let started = Instant::now();
        let result = connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        });
        let elapsed_us = started.elapsed().as_micros();
        if result
            .as_ref()
            .is_ok_and(|(busy, log, done)| *busy == 0 && log == done)
        {
            caught_up_at = Some(generation);
        }
        if timings_enabled || result.is_err() {
            let ended = unix_ms();
            let mut event = serde_json::json!({
                "event": "checkpoint", "kind": "idle", "mode": "PASSIVE",
                "start_unix_ms": ended.saturating_sub((elapsed_us / 1_000) as u64),
                "end_unix_ms": ended, "elapsed_us": elapsed_us,
                "activity_generation": generation,
                "activity_generation_after": activity.load(Ordering::Acquire),
            });
            match result {
                Ok((busy, log, done)) => {
                    event["busy"] = serde_json::json!(busy);
                    event["log_frames"] = serde_json::json!(log);
                    event["checkpointed_frames"] = serde_json::json!(done);
                }
                Err(error) => event["error"] = serde_json::json!(error.to_string()),
            }
            eprintln!("SQLITE_TIMING {event}");
        }
    }
}

struct CheckpointLifecycle {
    stop: std_mpsc::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CheckpointLifecycle {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Debug)]
pub enum DbError {
    Sqlite(rusqlite::Error),
    Serialize(serde_json::Error),
    InvalidUtf8(std::str::Utf8Error),
    WorkerStart(std::io::Error),
    WorkerUnavailable,
    WorkerBusy,
    ReadBatch(String),
    WriteBatch(String),
    InvalidPath,
    InvalidMmapSize,
    InvalidPageCacheSize,
    WalUnavailable,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "SQLite: {error}"),
            Self::Serialize(error) => write!(f, "JSON serialization: {error}"),
            Self::InvalidUtf8(error) => write!(f, "invalid post body UTF-8: {error}"),
            Self::WorkerStart(error) => write!(f, "database worker: {error}"),
            Self::WorkerUnavailable => f.write_str("database worker unavailable"),
            Self::WorkerBusy => f.write_str("database worker is at capacity"),
            Self::ReadBatch(message) => write!(f, "read batch failed: {message}"),
            Self::WriteBatch(message) => write!(f, "write batch failed: {message}"),
            Self::InvalidPath => f.write_str("database must be an existing on-disk file"),
            Self::InvalidMmapSize => {
                f.write_str("SQLite mmap size must fit in a signed 64-bit integer")
            }
            Self::InvalidPageCacheSize => f.write_str(
                "SQLite page cache must be positive, fit a signed 32-bit KiB setting, and cover 1 to 1,000,000,000 database pages",
            ),
            Self::WalUnavailable => f.write_str("database did not enable WAL mode"),
        }
    }
}

impl Error for DbError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Serialize(error) => Some(error),
            Self::InvalidUtf8(error) => Some(error),
            Self::WorkerStart(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

impl From<std::str::Utf8Error> for DbError {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(error)
    }
}

struct WorkerStatements<'connection> {
    begin_read: rusqlite::Statement<'connection>,
    begin_write: rusqlite::Statement<'connection>,
    commit_read: rusqlite::Statement<'connection>,
    rollback_read: rusqlite::Statement<'connection>,
    commit_write: rusqlite::Statement<'connection>,
    rollback_write: rusqlite::Statement<'connection>,
    savepoint: rusqlite::Statement<'connection>,
    rollback_savepoint: rusqlite::Statement<'connection>,
    release_savepoint: rusqlite::Statement<'connection>,
    health: rusqlite::Statement<'connection>,
    feed: rusqlite::Statement<'connection>,
    post: rusqlite::Statement<'connection>,
    create_post: rusqlite::Statement<'connection>,
    post_exists: rusqlite::Statement<'connection>,
    insert_like: rusqlite::Statement<'connection>,
}

impl<'connection> WorkerStatements<'connection> {
    fn prepare(connection: &'connection Connection) -> Result<Self, DbError> {
        // Persistent statements avoid exhausting SQLite's lookaside memory.
        Ok(Self {
            begin_read: connection
                .prepare_with_flags("BEGIN DEFERRED", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            begin_write: connection
                .prepare_with_flags("BEGIN IMMEDIATE", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            commit_read: connection
                .prepare_with_flags("COMMIT", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            rollback_read: connection
                .prepare_with_flags("ROLLBACK", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            commit_write: connection
                .prepare_with_flags("COMMIT", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            rollback_write: connection
                .prepare_with_flags("ROLLBACK", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            savepoint: connection.prepare_with_flags(
                "SAVEPOINT request_item",
                PrepFlags::SQLITE_PREPARE_PERSISTENT,
            )?,
            rollback_savepoint: connection.prepare_with_flags(
                "ROLLBACK TO request_item",
                PrepFlags::SQLITE_PREPARE_PERSISTENT,
            )?,
            release_savepoint: connection
                .prepare_with_flags("RELEASE request_item", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            health: connection
                .prepare_with_flags("SELECT 1", PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            feed: connection
                .prepare_with_flags(FEED_QUERY, PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            post: connection
                .prepare_with_flags(POST_QUERY, PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            create_post: connection
                .prepare_with_flags(CREATE_POST_QUERY, PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            post_exists: connection
                .prepare_with_flags(POST_EXISTS_QUERY, PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
            insert_like: connection
                .prepare_with_flags(INSERT_LIKE_QUERY, PrepFlags::SQLITE_PREPARE_PERSISTENT)?,
        })
    }
}

struct ReadBatchTransaction<'statement, 'connection> {
    connection: &'statement Connection,
    rollback: &'statement mut rusqlite::Statement<'connection>,
    active: bool,
}

impl<'statement, 'connection> ReadBatchTransaction<'statement, 'connection> {
    fn begin(
        connection: &'statement Connection,
        begin: &mut rusqlite::Statement<'connection>,
        rollback: &'statement mut rusqlite::Statement<'connection>,
    ) -> rusqlite::Result<Self> {
        begin.execute([])?;
        Ok(Self {
            connection,
            rollback,
            active: true,
        })
    }

    fn rollback(&mut self) -> rusqlite::Result<()> {
        if self.connection.is_autocommit() {
            self.active = false;
            return Ok(());
        }

        self.rollback.execute([])?;
        self.active = false;
        Ok(())
    }

    fn commit(mut self, commit: &mut rusqlite::Statement<'connection>) -> Result<(), String> {
        match commit.execute([]) {
            Ok(_) => {
                self.active = false;
                Ok(())
            }
            Err(error) => {
                let message = format!("commit failed: {error}");
                match self.rollback() {
                    Ok(()) => Err(message),
                    Err(rollback_error) => {
                        Err(format!("{message}; rollback failed: {rollback_error}"))
                    }
                }
            }
        }
    }
}

impl Drop for ReadBatchTransaction<'_, '_> {
    fn drop(&mut self) {
        if self.active
            && let Err(error) = self.rollback()
        {
            eprintln!("failed to roll back read batch transaction: {error}");
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T, DbError>>;

enum Request {
    Health(Reply<()>),
    Feed(Reply<Bytes>),
    Post(i64, Reply<Option<Vec<u8>>>),
    CreatePost {
        user_id: i64,
        body: Bytes,
        username: String,
        reply: Reply<Vec<u8>>,
    },
    Like {
        user_id: i64,
        post_id: i64,
        reply: Reply<Option<bool>>,
    },
}

impl Request {
    fn is_read(&self) -> bool {
        matches!(self, Self::Health(_) | Self::Feed(_) | Self::Post(_, _))
    }

    fn reply_closed(&self) -> bool {
        match self {
            Self::Health(reply) => reply.is_closed(),
            Self::Feed(reply) => reply.is_closed(),
            Self::Post(_, reply) => reply.is_closed(),
            Self::CreatePost { reply, .. } => reply.is_closed(),
            Self::Like { reply, .. } => reply.is_closed(),
        }
    }
}

// A producer unparks this worker after publishing a request. The thread's
// retained wake token closes the race between observing an empty queue and
// parking; spurious wakes simply recheck the queue and the same deadline.
fn receive_until(receiver: &mut mpsc::Receiver<Request>, deadline: Instant) -> Option<Request> {
    loop {
        let remaining = deadline.checked_duration_since(Instant::now())?;
        match receiver.try_recv() {
            Ok(request) => return Some(request),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => std::thread::park_timeout(remaining),
        }
    }
}

fn collect_read_batch(
    first: Request,
    receiver: &mut mpsc::Receiver<Request>,
    batch: &mut [Option<Request>; READ_BATCH_LIMIT],
) -> (usize, Option<Request>) {
    debug_assert!(first.is_read());
    let deadline = Instant::now() + BATCH_COLLECTION_WINDOW;
    batch[0] = Some(first);
    let mut len = 1;
    while len < READ_BATCH_LIMIT {
        match receive_until(receiver, deadline) {
            Some(request) if request.is_read() => {
                batch[len] = Some(request);
                len += 1;
            }
            Some(write) => return (len, Some(write)),
            None => return (len, None),
        }
    }
    (len, None)
}

enum ReadCompletion {
    Health(Reply<()>, Result<(), DbError>),
    Feed(Reply<Bytes>, Result<Bytes, DbError>),
    Post(Reply<Option<Vec<u8>>>, Result<Option<Vec<u8>>, DbError>),
}

impl ReadCompletion {
    fn fail(&mut self, message: &str) {
        match self {
            Self::Health(_, result) => *result = Err(DbError::ReadBatch(message.to_owned())),
            Self::Feed(_, result) => *result = Err(DbError::ReadBatch(message.to_owned())),
            Self::Post(_, result) => *result = Err(DbError::ReadBatch(message.to_owned())),
        }
    }

    fn send(self) {
        match self {
            Self::Health(reply, result) => {
                let _ = reply.send(result);
            }
            Self::Feed(reply, result) => {
                let _ = reply.send(result);
            }
            Self::Post(reply, result) => {
                let _ = reply.send(result);
            }
        }
    }
}

fn send_read_failure(request: Request, message: &str) {
    match request {
        Request::Health(reply) => {
            let _ = reply.send(Err(DbError::ReadBatch(message.to_owned())));
        }
        Request::Feed(reply) => {
            let _ = reply.send(Err(DbError::ReadBatch(message.to_owned())));
        }
        Request::Post(_, reply) => {
            let _ = reply.send(Err(DbError::ReadBatch(message.to_owned())));
        }
        Request::CreatePost { .. } | Request::Like { .. } => {
            unreachable!("only read requests are failed as a batch")
        }
    }
}

fn process_single_read(
    connection: &Connection,
    request: Request,
    statements: &mut WorkerStatements<'_>,
    feed_capacity: &mut usize,
    post_capacity: &mut usize,
) {
    match request {
        Request::Health(reply) => {
            let result = statements
                .health
                .query_row([], |_| Ok(()))
                .map_err(DbError::from);
            let _ = reply.send(result);
        }
        Request::Feed(reply) => {
            let result = match ReadBatchTransaction::begin(
                connection,
                &mut statements.begin_read,
                &mut statements.rollback_read,
            ) {
                Ok(transaction) => {
                    let mut result = read_feed(&mut statements.feed, feed_capacity);
                    if let Err(message) = transaction.commit(&mut statements.commit_read) {
                        result = Err(DbError::ReadBatch(message));
                    }
                    result
                }
                Err(error) => Err(DbError::ReadBatch(format!("begin failed: {error}"))),
            };
            let _ = reply.send(result);
        }
        Request::Post(id, reply) => {
            let _ = reply.send(read_post(&mut statements.post, id, post_capacity));
        }
        Request::CreatePost { .. } | Request::Like { .. } => {
            unreachable!("single-read path received a write")
        }
    }
}

fn process_read_batch(
    connection: &Connection,
    statements: &mut WorkerStatements<'_>,
    batch: &mut [Option<Request>; READ_BATCH_LIMIT],
    len: usize,
    feed_capacity: &mut usize,
    post_capacity: &mut usize,
) -> bool {
    let transaction = match ReadBatchTransaction::begin(
        connection,
        &mut statements.begin_read,
        &mut statements.rollback_read,
    ) {
        Ok(transaction) => transaction,
        Err(error) => {
            let message = format!("begin failed: {error}");
            for request in batch[..len].iter_mut().filter_map(Option::take) {
                if !request.reply_closed() {
                    send_read_failure(request, &message);
                }
            }
            return connection.is_autocommit();
        }
    };

    let mut completed: [Option<ReadCompletion>; READ_BATCH_LIMIT] = std::array::from_fn(|_| None);
    for index in 0..len {
        let Some(request) = batch[index].take() else {
            continue;
        };
        if request.reply_closed() {
            continue;
        }
        let completion = match request {
            Request::Health(reply) => {
                let result = statements
                    .health
                    .query_row([], |_| Ok(()))
                    .map_err(DbError::from);
                ReadCompletion::Health(reply, result)
            }
            Request::Feed(reply) => {
                ReadCompletion::Feed(reply, read_feed(&mut statements.feed, feed_capacity))
            }
            Request::Post(id, reply) => {
                let result = read_post(&mut statements.post, id, post_capacity);
                ReadCompletion::Post(reply, result)
            }
            Request::CreatePost { .. } | Request::Like { .. } => {
                unreachable!("read batch contains a write")
            }
        };
        completed[index] = Some(completion);
    }

    if let Err(message) = transaction.commit(&mut statements.commit_read) {
        for completion in completed.iter_mut().flatten() {
            completion.fail(&message);
        }
    }
    for completion in completed.into_iter().flatten() {
        completion.send();
    }
    connection.is_autocommit()
}

enum WriteCompletion {
    Health(Reply<()>, Result<(), DbError>),
    Feed(Reply<Bytes>, Result<Bytes, DbError>),
    Post(Reply<Option<Vec<u8>>>, Result<Option<Vec<u8>>, DbError>),
    CreatePost {
        reply: Reply<Vec<u8>>,
        result: Result<Vec<u8>, DbError>,
    },
    Like {
        reply: Reply<Option<bool>>,
        result: Result<Option<bool>, DbError>,
    },
}

impl WriteCompletion {
    fn fail_success(&mut self, message: &str) {
        match self {
            Self::Health(_, result) => {
                if result.is_ok() {
                    *result = Err(DbError::WriteBatch(message.to_owned()));
                }
            }
            Self::Feed(_, result) => {
                if result.is_ok() {
                    *result = Err(DbError::WriteBatch(message.to_owned()));
                }
            }
            Self::Post(_, result) => {
                if result.is_ok() {
                    *result = Err(DbError::WriteBatch(message.to_owned()));
                }
            }
            Self::CreatePost { result, .. } => {
                if result.is_ok() {
                    *result = Err(DbError::WriteBatch(message.to_owned()));
                }
            }
            Self::Like { result, .. } => {
                if result.is_ok() {
                    *result = Err(DbError::WriteBatch(message.to_owned()));
                }
            }
        }
    }

    fn send(self) {
        match self {
            Self::Health(reply, result) => {
                let _ = reply.send(result);
            }
            Self::Feed(reply, result) => {
                let _ = reply.send(result);
            }
            Self::Post(reply, result) => {
                let _ = reply.send(result);
            }
            Self::CreatePost { reply, result } => {
                let _ = reply.send(result);
            }
            Self::Like { reply, result } => {
                let _ = reply.send(result);
            }
        }
    }
}

fn collect_write_batch(
    first: Request,
    receiver: &mut mpsc::Receiver<Request>,
    batch: &mut [Option<Request>; WRITE_BATCH_LIMIT],
) -> usize {
    debug_assert!(!first.is_read());
    let deadline = Instant::now() + BATCH_COLLECTION_WINDOW;
    batch[0] = Some(first);
    let mut len = 1;
    while len < WRITE_BATCH_LIMIT {
        match receive_until(receiver, deadline) {
            Some(request) => {
                batch[len] = Some(request);
                len += 1;
            }
            None => break,
        }
    }
    len
}

struct WriteBatchCapacities<'a> {
    feed: &'a mut usize,
    post: &'a mut usize,
    created_post: &'a mut usize,
}

fn process_write_batch(
    connection: &Connection,
    statements: &mut WorkerStatements<'_>,
    batch: &mut [Option<Request>],
    capacities: WriteBatchCapacities<'_>,
    timings_enabled: bool,
) -> bool {
    let len = batch.len();
    assert!(len <= WRITE_BATCH_LIMIT);
    if batch.iter().flatten().all(Request::reply_closed) {
        for slot in batch.iter_mut() {
            drop(slot.take());
        }
        return connection.is_autocommit();
    }

    let mut completed: [Option<WriteCompletion>; WRITE_BATCH_LIMIT] = std::array::from_fn(|_| None);
    let begin_result = statements.begin_write.execute([]);
    if let Err(error) = begin_result {
        let message = format!("begin failed: {error}");
        for (index, slot) in batch[..len].iter_mut().enumerate() {
            if let Some(request) = slot.take()
                && !request.reply_closed()
            {
                completed[index] = Some(fail_write_request(request, &message));
            }
        }
        for completion in completed.into_iter().flatten() {
            completion.send();
        }
        return connection.is_autocommit();
    }

    let mut fatal: Option<String> = None;
    for index in 0..len {
        let Some(request) = batch[index].take() else {
            continue;
        };
        if request.reply_closed() {
            continue;
        }
        let is_write = !request.is_read();
        if is_write && let Err(error) = statements.savepoint.execute([]) {
            fatal = Some(format!("savepoint failed: {error}"));
            completed[index] = Some(fail_write_request(request, fatal.as_deref().unwrap()));
            break;
        }

        let result = match request {
            Request::Health(reply) => WriteCompletion::Health(
                reply,
                statements
                    .health
                    .query_row([], |_| Ok(()))
                    .map_err(DbError::from),
            ),
            Request::Feed(reply) => WriteCompletion::Feed(
                reply,
                read_feed(&mut statements.feed, &mut *capacities.feed),
            ),
            Request::Post(id, reply) => WriteCompletion::Post(
                reply,
                read_post(&mut statements.post, id, &mut *capacities.post),
            ),
            Request::CreatePost {
                user_id,
                body,
                username,
                reply,
            } => WriteCompletion::CreatePost {
                reply,
                result: insert_post(
                    &mut statements.create_post,
                    user_id,
                    body,
                    username,
                    &mut *capacities.created_post,
                ),
            },
            Request::Like {
                user_id,
                post_id,
                reply,
            } => WriteCompletion::Like {
                reply,
                result: insert_like(
                    &mut statements.post_exists,
                    &mut statements.insert_like,
                    user_id,
                    post_id,
                ),
            },
        };

        let item_failed = match &result {
            WriteCompletion::Health(_, r) => r.is_err(),
            WriteCompletion::Feed(_, r) => r.is_err(),
            WriteCompletion::Post(_, r) => r.is_err(),
            WriteCompletion::CreatePost { result: r, .. } => r.is_err(),
            WriteCompletion::Like { result: r, .. } => r.is_err(),
        };
        if is_write {
            let savepoint_result = if item_failed {
                statements
                    .rollback_savepoint
                    .execute([])
                    .and_then(|_| statements.release_savepoint.execute([]))
            } else {
                statements.release_savepoint.execute([])
            };
            if let Err(error) = savepoint_result {
                fatal = Some(format!("savepoint recovery failed: {error}"));
                completed[index] = Some(result);
                break;
            }
        }
        completed[index] = Some(result);
        if connection.is_autocommit() {
            fatal = Some("SQLite automatically rolled back the group transaction".to_owned());
            break;
        }
    }

    if fatal.is_none() {
        let commit_started = timings_enabled.then(Instant::now);
        if let Err(error) = statements.commit_write.execute([]) {
            fatal = Some(format!("commit failed: {error}"));
        }
        log_slow_commit(timings_enabled, commit_started, len);
    }
    if let Some(message) = fatal {
        if !connection.is_autocommit()
            && let Err(error) = statements.rollback_write.execute([])
        {
            let message = format!("{message}; rollback failed: {error}");
            for completion in completed.iter_mut().flatten() {
                completion.fail_success(&message);
            }
        } else {
            for completion in completed.iter_mut().flatten() {
                completion.fail_success(&message);
            }
        }
        for (index, slot) in batch[..len].iter_mut().enumerate() {
            if let Some(request) = slot.take()
                && !request.reply_closed()
            {
                completed[index] = Some(fail_write_request(request, &message));
            }
        }
    }
    for completion in completed.into_iter().flatten() {
        completion.send();
    }
    connection.is_autocommit()
}

fn fail_write_request(request: Request, message: &str) -> WriteCompletion {
    let error = || DbError::WriteBatch(message.to_owned());
    match request {
        Request::Health(reply) => WriteCompletion::Health(reply, Err(error())),
        Request::Feed(reply) => WriteCompletion::Feed(reply, Err(error())),
        Request::Post(_, reply) => WriteCompletion::Post(reply, Err(error())),
        Request::CreatePost { reply, .. } => WriteCompletion::CreatePost {
            reply,
            result: Err(error()),
        },
        Request::Like { reply, .. } => WriteCompletion::Like {
            reply,
            result: Err(error()),
        },
    }
}

#[derive(Clone)]
pub struct Db {
    requests: mpsc::Sender<Request>,
    worker_thread: std::thread::Thread,
    admission_timings: Option<Arc<AdmissionTimings>>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_read_batching(path, true)
    }

    pub fn open_with_read_batching(
        path: impl AsRef<Path>,
        read_batching: bool,
    ) -> Result<Self, DbError> {
        Self::open_with_read_batching_and_mmap_size(path, read_batching, 0)
    }

    pub fn open_with_read_batching_and_mmap_size(
        path: impl AsRef<Path>,
        read_batching: bool,
        mmap_size_bytes: u64,
    ) -> Result<Self, DbError> {
        Self::open_with_page_cache(path, read_batching, mmap_size_bytes, DEFAULT_PAGE_CACHE_KIB)
    }

    pub fn open_with_page_cache(
        path: impl AsRef<Path>,
        read_batching: bool,
        mmap_size_bytes: u64,
        page_cache_kib: u64,
    ) -> Result<Self, DbError> {
        let sqlite_timings = sqlite_timings_enabled();
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(DbError::InvalidPath);
        }
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::WalUnavailable);
        }
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "wal_autocheckpoint", 0)?;
        // Discard the old tail when SQLite safely resets a checkpointed WAL.
        // New frames then extend the file instead of reading cold overwritten pages.
        connection.pragma_update(None, "journal_size_limit", 0)?;
        configure_page_cache(&connection, page_cache_kib)?;
        let effective_mmap_size = configure_mmap_size(&connection, mmap_size_bytes)?;
        let checkpoint_connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        checkpoint_connection.busy_timeout(Duration::ZERO)?;
        checkpoint_connection.pragma_update(None, "synchronous", "NORMAL")?;
        checkpoint_connection.pragma_update(None, "wal_autocheckpoint", 0)?;
        eprintln!(
            "SQLite mmap_size requested={mmap_size_bytes} bytes effective={effective_mmap_size} bytes page_cache_target_kib={page_cache_kib} wal_autocheckpoint=0 checkpoint_mode=background_passive_when_idle idle_sample_seconds={}",
            CHECKPOINT_IDLE_INTERVAL.as_secs(),
        );
        let worker_activity = Arc::new(AtomicU64::new(0));

        let (checkpoint_stop_tx, checkpoint_stop_rx) = std_mpsc::channel();
        let checkpoint_timings = sqlite_timings;
        let checkpoint_activity = Arc::clone(&worker_activity);
        let checkpoint_worker = std::thread::Builder::new()
            .name("sqlite-checkpoint".into())
            .spawn(move || {
                checkpoint_worker(
                    checkpoint_connection,
                    checkpoint_stop_rx,
                    checkpoint_activity,
                    checkpoint_timings,
                );
            })
            .map_err(DbError::WorkerStart)?;
        let checkpoint_lifecycle = CheckpointLifecycle {
            stop: checkpoint_stop_tx,
            worker: Some(checkpoint_worker),
        };

        // The channel bounds queued requests at 4096; the worker additionally
        // dequeues at most one mixed write batch or read batch of 1024.
        let (requests, mut receiver) = mpsc::channel::<Request>(REQUEST_CAPACITY);
        let (startup_tx, startup_rx) = std_mpsc::channel();
        let worker_sqlite_timings = sqlite_timings;
        let worker = match std::thread::Builder::new()
            .name("sqlite".into())
            .spawn(move || {
                let _checkpoint_lifecycle = checkpoint_lifecycle;
                let mut statements = match WorkerStatements::prepare(&connection) {
                    Ok(statements) => {
                        let _ = startup_tx.send(Ok(()));
                        statements
                    }
                    Err(error) => {
                        let _ = startup_tx.send(Err(error));
                        return;
                    }
                };
                // Reuse the observed capacity, while each response owns its bytes.
                let mut feed_capacity = 4096;
                // Start single-post responses at 256 bytes and retain their
                // observed capacity as a numeric hint. Typical bodies fit this
                // size; larger or escaped bodies grow the hint when encountered.
                let mut post_capacity = 256;
                let mut created_post_capacity = 256;
                let mut timings = worker_sqlite_timings.then(WorkerTimings::new);
                let mut pending = None;
                let mut read_batch: [Option<Request>; READ_BATCH_LIMIT] =
                    std::array::from_fn(|_| None);
                while let Some(request) = pending.take().or_else(|| receiver.blocking_recv()) {
                    // The handler may have been cancelled while this request
                    // waited in the queue. Avoid its SQL and release its slot.
                    if request.reply_closed() {
                        continue;
                    }
                    worker_activity.fetch_add(1, Ordering::AcqRel);
                    let _activity = WorkerActivity(&worker_activity);
                    let batch_started = timings.as_ref().map(|_| Instant::now());
                    if request.is_read() && read_batching {
                        let (batch_len, boundary) =
                            collect_read_batch(request, &mut receiver, &mut read_batch);
                        let collected = timings.as_ref().map(|_| Instant::now());
                        pending = boundary;
                        let live = read_batch[..batch_len]
                            .iter()
                            .flatten()
                            .filter(|request| !request.reply_closed())
                            .count();
                        if live > 1 {
                            let connection_usable = process_read_batch(
                                &connection,
                                &mut statements,
                                &mut read_batch,
                                batch_len,
                                &mut feed_capacity,
                                &mut post_capacity,
                            );
                            if !connection_usable {
                                break;
                            }
                        } else {
                            let mut only_live = None;
                            for slot in &mut read_batch[..batch_len] {
                                if let Some(request) = slot.take()
                                    && !request.reply_closed()
                                {
                                    only_live = Some(request);
                                }
                            }
                            if let Some(request) = only_live {
                                process_single_read(
                                    &connection,
                                    request,
                                    &mut statements,
                                    &mut feed_capacity,
                                    &mut post_capacity,
                                );
                            }
                        }
                        if let (Some(timings), Some(started), Some(collected)) =
                            (&mut timings, batch_started, collected)
                        {
                            timings.record(false, batch_len, started, collected, receiver.len());
                        }
                        continue;
                    }
                    if request.is_read() {
                        process_single_read(
                            &connection,
                            request,
                            &mut statements,
                            &mut feed_capacity,
                            &mut post_capacity,
                        );
                        if let (Some(timings), Some(started)) = (&mut timings, batch_started) {
                            timings.record(false, 1, started, started, receiver.len());
                        }
                        continue;
                    }
                    let mut write_batch = std::array::from_fn(|_| None);
                    let batch_len = collect_write_batch(request, &mut receiver, &mut write_batch);
                    let collected = timings.as_ref().map(|_| Instant::now());
                    let capacities = WriteBatchCapacities {
                        feed: &mut feed_capacity,
                        post: &mut post_capacity,
                        created_post: &mut created_post_capacity,
                    };
                    let connection_usable = process_write_batch(
                        &connection,
                        &mut statements,
                        &mut write_batch[..batch_len],
                        capacities,
                        worker_sqlite_timings,
                    );
                    if let (Some(timings), Some(started), Some(collected)) =
                        (&mut timings, batch_started, collected)
                    {
                        timings.record(true, batch_len, started, collected, receiver.len());
                    }
                    if !connection_usable {
                        break;
                    }
                }
                // Finalize all retained statements before closing the connection.
                drop(statements);
                drop(connection);
            }) {
            Ok(worker) => worker,
            Err(error) => return Err(DbError::WorkerStart(error)),
        };
        let startup_result = startup_rx.recv().map_err(|_| DbError::WorkerUnavailable);
        match startup_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                drop(requests);
                let _ = worker.join();
                return Err(error);
            }
            Err(error) => {
                drop(requests);
                let _ = worker.join();
                return Err(error);
            }
        }
        Ok(Self {
            requests,
            worker_thread: worker.thread().clone(),
            admission_timings: sqlite_timings.then(|| Arc::new(AdmissionTimings::default())),
        })
    }

    async fn reserve_request(&self, write: bool) -> Result<mpsc::Permit<'_, Request>, DbError> {
        let started = self.admission_timings.as_ref().map(|_| Instant::now());
        // Let short bursts drain without growing the channel or waiting indefinitely.
        let result = tokio::time::timeout(REQUEST_ADMISSION_TIMEOUT, self.requests.reserve()).await;
        if let (Some(timings), Some(started)) = (&self.admission_timings, started) {
            timings.record(started, write, result.is_err(), self.requests.capacity());
        }
        result
            .map_err(|_| DbError::WorkerBusy)?
            .map_err(|_| DbError::WorkerUnavailable)
    }

    async fn request<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, DbError> {
        let permit = self.reserve_request(false).await?;
        let (reply, receiver) = oneshot::channel();
        permit.send(make(reply));
        self.worker_thread.unpark();
        receiver.await.map_err(|_| DbError::WorkerUnavailable)?
    }

    async fn write_request<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, DbError> {
        let permit = self.reserve_request(true).await?;
        let (reply, receiver) = oneshot::channel();
        permit.send(make(reply));
        self.worker_thread.unpark();
        receiver.await.map_err(|_| DbError::WorkerUnavailable)?
    }

    pub async fn health(&self) -> Result<(), DbError> {
        self.request(Request::Health).await
    }

    pub async fn feed(&self) -> Result<Bytes, DbError> {
        self.request(Request::Feed).await
    }

    pub async fn post(&self, id: i64) -> Result<Option<Vec<u8>>, DbError> {
        self.request(|reply| Request::Post(id, reply)).await
    }

    pub async fn create_post(
        &self,
        user_id: i64,
        body: Bytes,
        username: String,
    ) -> Result<Vec<u8>, DbError> {
        self.write_request(|reply| Request::CreatePost {
            user_id,
            body,
            username,
            reply,
        })
        .await
    }

    pub async fn like(&self, user_id: i64, post_id: i64) -> Result<Option<bool>, DbError> {
        self.write_request(|reply| Request::Like {
            user_id,
            post_id,
            reply,
        })
        .await
    }
}

fn configure_page_cache(connection: &Connection, page_cache_kib: u64) -> Result<(), DbError> {
    let page_cache_kib = i32::try_from(page_cache_kib)
        .ok()
        .filter(|size| *size > 0)
        .ok_or(DbError::InvalidPageCacheSize)?;
    let page_size: u32 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let pages = page_cache_kib as u64 * 1024 / u64::from(page_size);
    if !(1..=1_000_000_000).contains(&pages) {
        return Err(DbError::InvalidPageCacheSize);
    }
    connection.pragma_update(None, "cache_size", -i64::from(page_cache_kib))?;
    Ok(())
}

fn configure_mmap_size(connection: &Connection, requested_bytes: u64) -> Result<i64, DbError> {
    let requested_bytes = i64::try_from(requested_bytes).map_err(|_| DbError::InvalidMmapSize)?;
    connection.pragma_update(None, "mmap_size", requested_bytes)?;
    Ok(connection.query_row("PRAGMA mmap_size", [], |row| row.get(0))?)
}

fn decode_post<'a>(row: &'a rusqlite::Row<'_>) -> rusqlite::Result<Post<'a>> {
    Ok(Post {
        id: row.get(0)?,
        body: row.get_ref(1)?.as_str()?,
        created_at: row.get_ref(2)?.as_str()?,
        author: row.get_ref(3)?.as_str()?,
        like_count: row.get(4)?,
    })
}

fn read_feed(
    statement: &mut rusqlite::Statement<'_>,
    capacity: &mut usize,
) -> Result<Bytes, DbError> {
    let mut rows = statement.query([])?;
    let mut output = Vec::with_capacity(*capacity);
    output.extend_from_slice(b"{\"posts\":[");
    let mut first = true;
    while let Some(row) = rows.next()? {
        if !first {
            output.push(b',');
        }
        first = false;
        decode_post(row)?.write_json(&mut output)?;
    }
    drop(rows);
    output.extend_from_slice(b"]}");
    *capacity = output.capacity();
    Ok(Bytes::from(output))
}

fn read_post(
    statement: &mut rusqlite::Statement<'_>,
    id: i64,
    capacity: &mut usize,
) -> Result<Option<Vec<u8>>, DbError> {
    let mut rows = statement.query([id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut output = Vec::with_capacity(*capacity);
    output.extend_from_slice(b"{\"post\":");
    decode_post(row)?.write_json(&mut output)?;
    output.push(b'}');
    *capacity = output.capacity();
    Ok(Some(output))
}

fn insert_post(
    statement: &mut rusqlite::Statement<'_>,
    user_id: i64,
    body: Bytes,
    username: String,
    capacity: &mut usize,
) -> Result<Vec<u8>, DbError> {
    let body_text = std::str::from_utf8(&body)?;
    let output = {
        let mut rows = statement.query(params![user_id, body_text])?;
        let row = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let post = Post {
            id: row.get(0)?,
            body: body_text,
            created_at: row.get_ref(1)?.as_str().map_err(rusqlite::Error::from)?,
            author: &username,
            like_count: 0,
        };
        let mut output = Vec::with_capacity(*capacity);
        output.extend_from_slice(b"{\"post\":");
        post.write_json(&mut output)?;
        output.push(b'}');
        // Complete RETURNING before committing; borrowed row data is no longer used.
        rows.next()?;
        output
    };
    *capacity = output.capacity();
    Ok(output)
}

fn insert_like(
    exists_statement: &mut rusqlite::Statement<'_>,
    insert_statement: &mut rusqlite::Statement<'_>,
    user_id: i64,
    post_id: i64,
) -> Result<Option<bool>, DbError> {
    let inserted = match insert_statement.execute(params![user_id, post_id]) {
        Ok(inserted) => inserted,
        Err(error) => {
            // Let SQLite's foreign key check handle successful inserts. Only
            // classify a missing post after failure; invalid users stay errors.
            if error.sqlite_error().is_some_and(|error| {
                error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY
            }) {
                let exists: bool = exists_statement.query_row([post_id], |row| row.get(0))?;
                if !exists {
                    return Ok(None);
                }
            }
            return Err(error.into());
        }
    };
    if inserted != 0 {
        return Ok(Some(false));
    }

    // A no-op insert normally means a duplicate. Check the post so pre-existing
    // orphan likes still return 404.
    let exists: bool = exists_statement.query_row([post_id], |row| row.get(0))?;
    if !exists {
        return Ok(None);
    }
    Ok(Some(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(bytes: impl AsRef<[u8]>) -> serde_json::Value {
        serde_json::from_slice(bytes.as_ref()).unwrap()
    }

    fn test_directory() -> tempfile::TempDir {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
        let directory = test_directory();
        let path = directory.path().join("feed.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../../schema.sql"))
            .unwrap();
        connection
            .execute(
                "INSERT INTO users (id, username) VALUES (1, 'alice'), (2, 'bob')",
                [],
            )
            .unwrap();
        (directory, path)
    }

    async fn assert_feed_matches_sql_reference(db: &Db, path: &Path) {
        let connection = Connection::open(path).unwrap();
        let mut statement = connection.prepare(FEED_REFERENCE_QUERY).unwrap();
        let mut rows = statement.query([]).unwrap();
        let mut posts = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            posts.push(
                serde_json::to_value(Post {
                    id: row.get(0).unwrap(),
                    body: row.get_ref(1).unwrap().as_str().unwrap(),
                    created_at: row.get_ref(2).unwrap().as_str().unwrap(),
                    author: row.get_ref(3).unwrap().as_str().unwrap(),
                    like_count: row.get(4).unwrap(),
                })
                .unwrap(),
            );
        }
        drop(rows);
        drop(statement);
        drop(connection);
        let expected = serde_json::json!({ "posts": posts });
        assert_eq!(json(db.feed().await.unwrap()), expected);
    }

    #[tokio::test]
    async fn feed_single_query_matches_ordered_reference_and_edge_cases() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        assert_feed_matches_sql_reference(&db, &path).await;

        let connection = Connection::open(&path).unwrap();
        for id in 1..=7 {
            connection
                .execute(
                    "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'short', ?2)",
                    params![id, format!("2025-01-01T00:00:{id:02}.000Z")],
                )
                .unwrap();
        }
        drop(connection);
        assert_feed_matches_sql_reference(&db, &path).await;

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("DELETE FROM likes; DELETE FROM posts;")
            .unwrap();
        for id in 100..120 {
            let seconds = (id * 7) % 20;
            connection
                .execute(
                    "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, ?2, ?3)",
                    params![
                        id,
                        format!("body {id}"),
                        format!("2025-01-01T00:00:{seconds:02}.000Z")
                    ],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO likes (user_id, post_id) VALUES (1, 104), (2, 104), (2, 117)",
                [],
            )
            .unwrap();
        drop(connection);
        assert_feed_matches_sql_reference(&db, &path).await;

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("DELETE FROM likes; DELETE FROM posts;")
            .unwrap();
        for id in (200..240).step_by(2) {
            connection
                .execute(
                    "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'sparse', '2025-01-01T00:00:00.000Z')",
                    [id],
                )
                .unwrap();
        }
        drop(connection);
        assert_feed_matches_sql_reference(&db, &path).await;

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("DELETE FROM likes; DELETE FROM posts;")
            .unwrap();
        for id in 300..320 {
            connection
                .execute(
                    "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'tied', '2025-01-01T00:00:00.000Z')",
                    [id],
                )
                .unwrap();
        }
        connection
            .execute(
                "INSERT INTO likes (user_id, post_id) VALUES (1, 303), (2, 303), (2, 305)",
                [],
            )
            .unwrap();
        drop(connection);
        assert_feed_matches_sql_reference(&db, &path).await;

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("DELETE FROM likes; DELETE FROM posts;")
            .unwrap();
        for id in 400..420 {
            connection
                .execute(
                    "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'valid', '2025-01-01T00:00:00.000Z')",
                    [id],
                )
                .unwrap();
        }
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .unwrap();
        connection
            .execute(
                "INSERT INTO posts (id, user_id, body, created_at) VALUES (420, 999, 'orphan', '2026-01-01T00:00:00.000Z')",
                [],
            )
            .unwrap();
        drop(connection);
        assert_feed_matches_sql_reference(&db, &path).await;
    }

    #[tokio::test]
    async fn committed_posts_preserve_utf8_and_token_author_and_survive_reopen() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let body = "héllo 🦀\n\"quoted\"";
        let created = json(
            db.create_post(1, body.into(), "token-name".into())
                .await
                .unwrap(),
        );
        let created = &created["post"];
        let id = created["id"].as_i64().unwrap();
        assert_eq!(created["body"], body);
        assert_eq!(created["author"], "token-name");
        assert_eq!(created["like_count"], 0);
        let timestamp = created["created_at"].as_str().unwrap();
        assert_eq!(timestamp.len(), 24);
        assert!(timestamp.ends_with('Z'));
        assert_eq!(db.like(2, id).await.unwrap(), Some(false));
        drop(db);

        let reopened = Db::open(&path).unwrap();
        let fetched = json(reopened.post(id).await.unwrap().unwrap());
        let fetched = &fetched["post"];
        assert_eq!(fetched["body"], body);
        assert_eq!(fetched["created_at"], timestamp);
        assert_eq!(fetched["author"], "alice");
        assert_eq!(fetched["like_count"], 1);
    }

    #[tokio::test]
    async fn committed_data_remains_available_after_reopen() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let mut last_id = 0;
        for index in 0..32 {
            let created = json(
                db.create_post(1, format!("checkpoint-{index}").into(), "alice".into())
                    .await
                    .unwrap(),
            );
            last_id = created["post"]["id"].as_i64().unwrap();
        }

        drop(db);
        let reopened = Db::open(&path).unwrap();
        let fetched = json(reopened.post(last_id).await.unwrap().unwrap());
        assert_eq!(fetched["post"]["body"], "checkpoint-31");
    }

    #[test]
    fn mmap_size_reports_the_effective_value_from_the_configured_connection() {
        let (_directory, path) = fixture();
        let connection = Connection::open(path).unwrap();
        let requested_bytes = 64 * 1024 * 1024;

        let effective_bytes = configure_mmap_size(&connection, requested_bytes).unwrap();
        let queried_bytes: i64 = connection
            .query_row("PRAGMA mmap_size", [], |row| row.get(0))
            .unwrap();

        assert_eq!(effective_bytes, queried_bytes);
        assert!(effective_bytes > 0);
        assert!(effective_bytes <= requested_bytes as i64);
        assert!(matches!(
            configure_mmap_size(&connection, u64::MAX),
            Err(DbError::InvalidMmapSize)
        ));
    }

    #[tokio::test]
    async fn duplicate_likes_are_atomic_and_each_read_gets_current_data() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let post = json(
            db.create_post(1, "body".into(), "alice".into())
                .await
                .unwrap(),
        );
        let id = post["post"]["id"].as_i64().unwrap();
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            0
        );
        let (first, second) = tokio::join!(db.like(2, id), db.like(2, id));
        assert_eq!((first.unwrap(), second.unwrap()), (Some(false), Some(true)));
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            1
        );
        assert_eq!(json(db.feed().await.unwrap())["posts"][0]["like_count"], 1);

        let external = Connection::open(&path).unwrap();
        external
            .execute("INSERT INTO likes (user_id, post_id) VALUES (1, ?1)", [id])
            .unwrap();
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            2
        );
        assert_eq!(json(db.feed().await.unwrap())["posts"][0]["like_count"], 2);
        assert_eq!(db.like(1, id + 1).await.unwrap(), None);
        assert!(db.post(id + 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn feed_orders_timestamp_then_id_and_limits_to_twenty() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        for id in 1..=22 {
            connection.execute(
                "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'body', '2025-01-01T00:00:00.000Z')",
                [id],
            ).unwrap();
        }
        connection
            .execute(
                "UPDATE posts SET created_at = '2026-01-01T00:00:00.000Z' WHERE id = 1",
                [],
            )
            .unwrap();
        let feed = json(Db::open(&path).unwrap().feed().await.unwrap());
        let feed = feed["posts"].as_array().unwrap();
        assert_eq!(feed.len(), 20);
        assert_eq!(
            feed.iter()
                .map(|post| post["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            std::iter::once(1).chain((4..=22).rev()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn sqlite_constraints_fail_without_poisoning_the_worker() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        assert!(
            db.create_post(999, "body".into(), "nobody".into())
                .await
                .is_err()
        );
        assert!(
            db.create_post(1, Bytes::new(), "alice".into())
                .await
                .is_err()
        );
        assert!(matches!(
            db.create_post(1, Bytes::from_static(&[0xff]), "alice".into())
                .await,
            Err(DbError::InvalidUtf8(_))
        ));
        let post = json(
            db.create_post(1, "body".into(), "alice".into())
                .await
                .unwrap(),
        );
        let id = post["post"]["id"].as_i64().unwrap();
        assert!(db.like(999, id).await.is_err());
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            0
        );
        db.health().await.unwrap();
    }

    #[tokio::test]
    async fn borrowed_rows_produce_exact_compact_json_and_escape_strings() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        assert_eq!(db.feed().await.unwrap().as_ref(), br#"{"posts":[]}"#);
        let connection = Connection::open(&path).unwrap();
        connection.execute(
            "INSERT INTO posts (id, user_id, body, created_at) VALUES (1, 1, ?1, '2025-01-01T00:00:00.000Z')",
            ["héllo 🦀\n\"quoted\"\\\t"],
        ).unwrap();
        let post = db.post(1).await.unwrap().unwrap();
        assert_eq!(post, r#"{"post":{"id":1,"body":"héllo 🦀\n\"quoted\"\\\t","created_at":"2025-01-01T00:00:00.000Z","author":"alice","like_count":0}}"#.as_bytes());
        let feed = db.feed().await.unwrap();
        assert_eq!(feed, r#"{"posts":[{"id":1,"body":"héllo 🦀\n\"quoted\"\\\t","created_at":"2025-01-01T00:00:00.000Z","author":"alice","like_count":0}]}"#.as_bytes());
    }

    #[test]
    fn open_requires_existing_disk_database() {
        let directory = test_directory();
        let missing = directory.path().join("missing.db");
        assert!(Db::open(&missing).is_err());
        assert!(!missing.exists());
        assert!(matches!(Db::open(":memory:"), Err(DbError::InvalidPath)));
        assert!(matches!(Db::open(""), Err(DbError::InvalidPath)));
    }

    #[tokio::test]
    async fn closed_worker_returns_an_error() {
        let (requests, receiver) = mpsc::channel(1);
        drop(receiver);
        let db = Db {
            requests,
            worker_thread: std::thread::current(),
            admission_timings: None,
        };
        assert!(matches!(db.health().await, Err(DbError::WorkerUnavailable)));
    }

    #[tokio::test]
    async fn outstanding_requests_time_out_when_the_queue_stays_full() {
        let (requests, mut receiver) = mpsc::channel(1);
        let db = Db {
            requests,
            worker_thread: std::thread::current(),
            admission_timings: None,
        };
        let first_db = db.clone();
        let first = tokio::spawn(async move { first_db.health().await });
        let active = receiver.recv().await.unwrap();

        let second_db = db.clone();
        let second = tokio::spawn(async move { second_db.health().await });
        while db.requests.capacity() != 0 {
            tokio::task::yield_now().await;
        }

        let started = Instant::now();
        assert!(matches!(db.health().await, Err(DbError::WorkerBusy)));
        assert!(started.elapsed() >= REQUEST_ADMISSION_TIMEOUT);

        drop(active);
        drop(receiver.recv().await.unwrap());
        assert!(matches!(
            first.await.unwrap(),
            Err(DbError::WorkerUnavailable)
        ));
        assert!(matches!(
            second.await.unwrap(),
            Err(DbError::WorkerUnavailable)
        ));
        let retry = tokio::spawn({
            let db = db.clone();
            async move { db.health().await }
        });
        drop(receiver.recv().await.unwrap());
        assert!(matches!(
            retry.await.unwrap(),
            Err(DbError::WorkerUnavailable)
        ));
    }

    #[tokio::test]
    async fn read_and_write_admission_wait_for_queue_capacity() {
        for write in [false, true] {
            let (requests, mut receiver) = mpsc::channel(1);
            let db = Db {
                requests,
                worker_thread: std::thread::current(),
                admission_timings: None,
            };
            let (reply, _result) = oneshot::channel();
            db.requests.try_send(Request::Health(reply)).unwrap();
            let waiting = async {
                if write {
                    db.write_request(Request::Health).await
                } else {
                    db.health().await
                }
            };
            tokio::pin!(waiting);
            tokio::select! {
                biased;
                result = &mut waiting => panic!("full queue should wait: {result:?}"),
                _ = std::future::ready(()) => {}
            }
            drop(receiver.recv().await.unwrap());
            let (result, ()) = tokio::join!(waiting, async {
                let Request::Health(reply) = receiver.recv().await.unwrap() else {
                    panic!("expected health request");
                };
                reply.send(Ok(())).unwrap();
            });
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_queued_write_is_skipped_when_worker_drains_it() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let (first_reply, first_result) = oneshot::channel();
        db.requests
            .try_send(Request::CreatePost {
                user_id: 1,
                body: "active".into(),
                username: "alice".into(),
                reply: first_reply,
            })
            .unwrap();
        while db.requests.capacity() != REQUEST_CAPACITY {
            tokio::task::yield_now().await;
        }

        let queued = tokio::spawn({
            let db = db.clone();
            async move { db.create_post(1, "cancelled".into(), "alice".into()).await }
        });
        while db.requests.capacity() != REQUEST_CAPACITY - 1 {
            tokio::task::yield_now().await;
        }

        queued.abort();
        let _ = queued.await;
        blocker.execute_batch("ROLLBACK").unwrap();
        first_result.await.unwrap().unwrap();
        db.health().await.unwrap();

        let connection = Connection::open(&path).unwrap();
        let posts: Vec<String> = connection
            .prepare("SELECT body FROM posts ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(posts, ["active"]);
    }

    #[test]
    fn read_batch_stops_at_write_and_preserves_fifo_order() {
        let (sender, mut receiver) = mpsc::channel(8);
        let (reply, _response) = oneshot::channel();
        sender.try_send(Request::Post(1, reply)).unwrap();
        let (reply, _response) = oneshot::channel();
        sender.try_send(Request::Feed(reply)).unwrap();
        let (reply, _response) = oneshot::channel();
        sender
            .try_send(Request::CreatePost {
                user_id: 7,
                body: Bytes::new(),
                username: String::new(),
                reply,
            })
            .unwrap();
        let (reply, _response) = oneshot::channel();
        sender.try_send(Request::Post(3, reply)).unwrap();

        let first = receiver.try_recv().unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (len, pending) = collect_read_batch(first, &mut receiver, &mut batch);

        assert_eq!(len, 2);
        assert!(matches!(batch[0], Some(Request::Post(1, _))));
        assert!(matches!(batch[1], Some(Request::Feed(_))));
        assert!(matches!(
            pending,
            Some(Request::CreatePost { user_id: 7, .. })
        ));
        assert!(matches!(receiver.try_recv(), Ok(Request::Post(3, _))));
    }

    #[test]
    fn read_batch_is_bounded_and_never_waits_for_more_requests() {
        let (sender, mut receiver) = mpsc::channel(READ_BATCH_LIMIT + 2);
        for id in 0..(READ_BATCH_LIMIT + 2) {
            let (reply, _response) = oneshot::channel();
            sender.try_send(Request::Post(id as i64, reply)).unwrap();
        }

        let first = receiver.try_recv().unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (len, pending) = collect_read_batch(first, &mut receiver, &mut batch);

        assert_eq!(len, READ_BATCH_LIMIT);
        assert!(pending.is_none());
        assert!(matches!(batch[0], Some(Request::Post(0, _))));
        assert!(matches!(
            batch[READ_BATCH_LIMIT - 1],
            Some(Request::Post(id, _)) if id == READ_BATCH_LIMIT as i64 - 1
        ));
        assert!(
            matches!(receiver.try_recv(), Ok(Request::Post(id, _)) if id == READ_BATCH_LIMIT as i64)
        );
        assert!(
            matches!(receiver.try_recv(), Ok(Request::Post(id, _)) if id == READ_BATCH_LIMIT as i64 + 1)
        );

        // An empty queue ends collection when the bounded collection window
        // expires; no later request is needed to complete the batch.
        let (reply, _response) = oneshot::channel();
        let (len, pending) =
            collect_read_batch(Request::Post(10, reply), &mut receiver, &mut batch);
        assert_eq!(len, 1);
        assert!(pending.is_none());
    }

    #[test]
    fn write_batch_is_bounded_and_keeps_mixed_fifo_snapshot() {
        let (sender, mut receiver) = mpsc::channel(WRITE_BATCH_LIMIT + 2);
        let mut responses = Vec::new();
        for index in 0..(WRITE_BATCH_LIMIT + 1) {
            let (reply, response) = oneshot::channel();
            responses.push(response);
            sender
                .try_send(Request::Like {
                    user_id: 1,
                    post_id: index as i64,
                    reply,
                })
                .unwrap();
        }
        let first = receiver.try_recv().unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let len = collect_write_batch(first, &mut receiver, &mut batch);
        assert_eq!(len, WRITE_BATCH_LIMIT);
        assert!(matches!(batch[0], Some(Request::Like { post_id: 0, .. })));
        assert!(matches!(
            batch[WRITE_BATCH_LIMIT - 1],
            Some(Request::Like { post_id, .. }) if post_id == WRITE_BATCH_LIMIT as i64 - 1
        ));
        assert!(matches!(
            receiver.try_recv(),
            Ok(Request::Like { post_id, .. }) if post_id == WRITE_BATCH_LIMIT as i64
        ));
        drop(sender);
        drop(responses);
    }

    #[test]
    fn write_batch_preserves_valid_writes_around_item_failure_and_read_order() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        let mut batch: [Option<Request>; WRITE_BATCH_LIMIT] = std::array::from_fn(|_| None);
        let (first_reply, first_response) = oneshot::channel();
        let (bad_reply, bad_response) = oneshot::channel();
        let (feed_reply, feed_response) = oneshot::channel();
        let (last_reply, last_response) = oneshot::channel();
        batch[0] = Some(Request::CreatePost {
            user_id: 1,
            body: "first".into(),
            username: "alice".into(),
            reply: first_reply,
        });
        batch[1] = Some(Request::CreatePost {
            user_id: 1,
            body: Bytes::from_static(b"\xff"),
            username: "alice".into(),
            reply: bad_reply,
        });
        batch[2] = Some(Request::Feed(feed_reply));
        batch[3] = Some(Request::CreatePost {
            user_id: 2,
            body: "last".into(),
            username: "bob".into(),
            reply: last_reply,
        });
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;
        let mut created_post_capacity = 256;

        assert!(process_write_batch(
            &connection,
            &mut statements,
            &mut batch[..4],
            WriteBatchCapacities {
                feed: &mut feed_capacity,
                post: &mut post_capacity,
                created_post: &mut created_post_capacity,
            },
            false
        ));
        assert_eq!(
            json(first_response.blocking_recv().unwrap().unwrap())["post"]["body"],
            "first"
        );
        assert!(matches!(
            bad_response.blocking_recv().unwrap(),
            Err(DbError::InvalidUtf8(_))
        ));
        let feed = json(feed_response.blocking_recv().unwrap().unwrap());
        assert_eq!(feed["posts"][0]["body"], "first");
        assert_eq!(
            json(last_response.blocking_recv().unwrap().unwrap())["post"]["body"],
            "last"
        );
        let count: i64 = connection
            .query_row("SELECT count(*) FROM posts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn write_batch_commit_failure_rolls_back_all_successful_items() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        statements.commit_write = connection
            .prepare("SELECT abs(-9223372036854775808)")
            .unwrap();
        let mut batch: [Option<Request>; WRITE_BATCH_LIMIT] = std::array::from_fn(|_| None);
        let (reply, response) = oneshot::channel();
        batch[0] = Some(Request::CreatePost {
            user_id: 1,
            body: "rolled back".into(),
            username: "alice".into(),
            reply,
        });
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;
        let mut created_post_capacity = 256;

        assert!(process_write_batch(
            &connection,
            &mut statements,
            &mut batch[..1],
            WriteBatchCapacities {
                feed: &mut feed_capacity,
                post: &mut post_capacity,
                created_post: &mut created_post_capacity,
            },
            false
        ));
        assert!(
            matches!(response.blocking_recv().unwrap(), Err(DbError::WriteBatch(message)) if message.starts_with("commit failed:"))
        );
        let count: i64 = connection
            .query_row("SELECT count(*) FROM posts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert!(connection.is_autocommit());
    }

    #[tokio::test]
    async fn cancelled_queued_read_is_skipped_before_the_next_read() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let (active_reply, active_result) = oneshot::channel();
        db.requests
            .try_send(Request::CreatePost {
                user_id: 1,
                body: "active".into(),
                username: "alice".into(),
                reply: active_reply,
            })
            .unwrap();
        while db.requests.capacity() != REQUEST_CAPACITY {
            tokio::task::yield_now().await;
        }

        let (cancelled_reply, cancelled_result) = oneshot::channel();
        db.requests
            .try_send(Request::Post(1, cancelled_reply))
            .unwrap();
        drop(cancelled_result);
        let (live_reply, live_result) = oneshot::channel();
        db.requests.try_send(Request::Post(1, live_reply)).unwrap();
        while db.requests.capacity() != REQUEST_CAPACITY - 2 {
            tokio::task::yield_now().await;
        }

        blocker.execute_batch("ROLLBACK").unwrap();
        active_result.await.unwrap().unwrap();
        assert!(live_result.await.unwrap().unwrap().is_some());
        db.health().await.unwrap();
    }

    #[tokio::test]
    async fn read_batch_keeps_each_request_response_independent() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "ignore_check_constraints", "ON")
            .unwrap();
        connection
            .execute(
                "INSERT INTO posts (id, user_id, body, created_at) VALUES (1, 1, ?1, '2025-01-01T00:00:00.000Z'), (2, 2, 'two', '2025-01-02T00:00:00.000Z')",
                [rusqlite::types::Value::Blob(vec![0xff])],
            )
            .unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (first_reply, first_response) = oneshot::channel();
        let (second_reply, second_response) = oneshot::channel();
        let (health_reply, health_response) = oneshot::channel();
        batch[0] = Some(Request::Post(1, first_reply));
        batch[1] = Some(Request::Post(2, second_reply));
        batch[2] = Some(Request::Health(health_reply));
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;

        assert!(process_read_batch(
            &connection,
            &mut statements,
            &mut batch,
            3,
            &mut feed_capacity,
            &mut post_capacity,
        ));

        assert!(matches!(
            first_response.await.unwrap(),
            Err(DbError::Sqlite(_))
        ));
        let second = json(second_response.await.unwrap().unwrap().unwrap());
        assert_eq!(second["post"]["id"], 2);
        assert_eq!(second["post"]["body"], "two");
        health_response.await.unwrap().unwrap();
        assert!(connection.is_autocommit());
    }

    #[test]
    fn read_batch_begin_error_is_reported_without_rolling_back_existing_transaction() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        connection.execute_batch("BEGIN").unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (first_reply, first_response) = oneshot::channel();
        let (second_reply, second_response) = oneshot::channel();
        batch[0] = Some(Request::Health(first_reply));
        batch[1] = Some(Request::Health(second_reply));
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;

        let reusable = process_read_batch(
            &connection,
            &mut statements,
            &mut batch,
            2,
            &mut feed_capacity,
            &mut post_capacity,
        );

        for response in [first_response, second_response] {
            let Err(DbError::ReadBatch(message)) = response.blocking_recv().unwrap() else {
                panic!("begin failure should be reported to each read");
            };
            assert!(message.starts_with("begin failed:"));
        }
        assert!(!connection.is_autocommit());
        assert!(!reusable);
        connection.execute_batch("ROLLBACK").unwrap();
        assert!(connection.is_autocommit());
    }

    #[test]
    fn read_batch_commit_error_is_reported_after_transaction_is_rolled_back() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        let transaction = ReadBatchTransaction::begin(
            &connection,
            &mut statements.begin_read,
            &mut statements.rollback_read,
        )
        .unwrap();
        connection.execute_batch("ROLLBACK").unwrap();

        let error = transaction.commit(&mut statements.commit_read).unwrap_err();

        assert!(error.starts_with("commit failed:"));
        assert!(connection.is_autocommit());
    }

    #[test]
    fn read_batch_stops_using_connection_when_commit_and_rollback_fail() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        statements.commit_read = connection
            .prepare("SELECT abs(-9223372036854775808)")
            .unwrap();
        statements.rollback_read = connection
            .prepare("SELECT abs(-9223372036854775808)")
            .unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (first_reply, first_response) = oneshot::channel();
        let (second_reply, second_response) = oneshot::channel();
        batch[0] = Some(Request::Health(first_reply));
        batch[1] = Some(Request::Health(second_reply));
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;

        let reusable = process_read_batch(
            &connection,
            &mut statements,
            &mut batch,
            2,
            &mut feed_capacity,
            &mut post_capacity,
        );

        for response in [first_response, second_response] {
            let Err(DbError::ReadBatch(message)) = response.blocking_recv().unwrap() else {
                panic!("failed controls should report the commit error to each read");
            };
            assert!(message.starts_with("commit failed:"));
            assert!(message.contains("rollback failed:"));
        }
        assert!(!reusable);
        assert!(!connection.is_autocommit());
        connection.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn read_batch_begin_error_with_autocommit_connection_is_recoverable() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();
        statements.begin_read = connection
            .prepare("SELECT abs(-9223372036854775808)")
            .unwrap();
        let mut batch = std::array::from_fn(|_| None);
        let (reply, response) = oneshot::channel();
        batch[0] = Some(Request::Health(reply));
        let mut feed_capacity = 4096;
        let mut post_capacity = 256;

        let reusable = process_read_batch(
            &connection,
            &mut statements,
            &mut batch,
            1,
            &mut feed_capacity,
            &mut post_capacity,
        );

        let Err(DbError::ReadBatch(message)) = response.blocking_recv().unwrap() else {
            panic!("failed begin control should report an error");
        };
        assert!(message.starts_with("begin failed:"));
        assert!(reusable);
        assert!(connection.is_autocommit());
    }

    #[test]
    fn read_batch_transaction_drop_rolls_back_during_unwind() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        let mut statements = WorkerStatements::prepare(&connection).unwrap();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _transaction = ReadBatchTransaction::begin(
                &connection,
                &mut statements.begin_read,
                &mut statements.rollback_read,
            )
            .unwrap();
            panic!("simulated read batch panic");
        }));

        assert!(panic.is_err());
        assert!(connection.is_autocommit());
    }

    #[test]
    fn open_reports_fixed_statement_prepare_errors() {
        let directory = test_directory();
        let path = directory.path().join("empty.db");
        Connection::open(&path).unwrap();

        assert!(matches!(Db::open(path), Err(DbError::Sqlite(_))));
    }
}
