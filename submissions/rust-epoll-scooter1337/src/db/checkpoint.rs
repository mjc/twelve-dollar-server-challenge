use super::*;
use std::{sync::mpsc, thread, time::Duration};

pub(super) struct Checkpointer {
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}

// Each connection stays on one thread. SQLITE_THREADSAFE=2 protects SQLite's
// shared state; NORMAL locking permits this connection to backfill the WAL.
struct CheckpointConnection(*mut Connection);
impl Drop for CheckpointConnection {
    fn drop(&mut self) {
        // SAFETY: this thread owns the connection and has no live statements.
        assert_eq!(unsafe { sqlite3_close(self.0) }, 0);
    }
}

impl Checkpointer {
    pub(super) fn start(path: CString, frames: Arc<AtomicI32>) -> Self {
        let (stop, receiver) = mpsc::channel();
        let (ready, started) = mpsc::sync_channel(0);
        let worker = thread::Builder::new()
            .name("sqlite-checkpoint".into())
            .spawn(move || {
                let mut raw = ptr::null_mut();
                // SAFETY: the path and output pointer are valid for this call.
                // This connection is created, used and closed on this thread.
                assert_eq!(unsafe { sqlite3_open_v2(path.as_ptr(), &mut raw, 2 | 0x8000, ptr::null()) }, 0);
                let connection = CheckpointConnection(raw);
                let sql = c"PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=500; SELECT count(*) FROM sqlite_schema;";
                // SAFETY: connection and SQL are live, and callbacks are disabled.
                assert_eq!(unsafe { sqlite3_exec(connection.0, sql.as_ptr(), ptr::null(), ptr::null_mut(), ptr::null_mut()) }, 0);
                ready.send(()).expect("checkpoint startup receiver");
                while matches!(receiver.recv_timeout(Duration::from_secs(1)), Err(mpsc::RecvTimeoutError::Timeout)) {
                    let current = frames.load(Ordering::Relaxed);
                    if current < 1000 {
                        continue;
                    }
                    let mut log = 0;
                    let mut backfilled = 0;
                    // SAFETY: this thread exclusively owns the connection. Both
                    // output pointers are valid. PASSIVE never waits for readers
                    // or the writer to release their locks (busy timeout is zero).
                    let rc = unsafe { sqlite3_wal_checkpoint_v2(connection.0, ptr::null(), 0, &mut log, &mut backfilled) };
                    if rc != 0 && rc != 5 {
                        eprintln!("background SQLite checkpoint failed: {rc}");
                    }
                }
            })
            .expect("start SQLite checkpointer");
        started.recv().expect("initialize SQLite checkpointer");
        Self {
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        self.worker
            .take()
            .unwrap()
            .join()
            .expect("SQLite checkpointer panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{ffi::CStr, time::Instant};

    fn open(path: &CString) -> CheckpointConnection {
        let mut raw = ptr::null_mut();
        // SAFETY: valid input/output pointers; the test owns the new connection.
        assert_eq!(
            unsafe { sqlite3_open_v2(path.as_ptr(), &mut raw, 6 | 0x8000, ptr::null()) },
            0
        );
        CheckpointConnection(raw)
    }

    fn exec(connection: &CheckpointConnection, sql: &CStr) {
        // SAFETY: the connection is exclusively borrowed and SQL is terminated.
        assert_eq!(
            unsafe {
                sqlite3_exec(
                    connection.0,
                    sql.as_ptr(),
                    ptr::null(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                )
            },
            0
        );
    }

    fn progress(db: &Db) -> Option<(i32, i32)> {
        let (mut log, mut backfilled) = (0, 0);
        // SAFETY: the test owns the request connection; outputs are valid. NOOP
        // only reads progress, leaving the background worker to do the backfill.
        let rc = unsafe {
            sqlite3_wal_checkpoint_v2(db.raw, ptr::null(), -1, &mut log, &mut backfilled)
        };
        assert!(rc == 0 || rc == 5, "checkpoint progress: {rc}");
        (rc == 0).then_some((log, backfilled))
    }

    fn wait_for(db: &Db, predicate: impl Fn(i32, i32) -> bool) -> (i32, i32) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some((log, backfilled)) = progress(db) {
                if predicate(log, backfilled) {
                    return (log, backfilled);
                }
            }
            assert!(
                Instant::now() < deadline,
                "background checkpoint did not make progress"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn background_checkpoint_preserves_reader_and_catches_up_after_release() {
        let fixture =
            std::env::temp_dir().join(format!("scooter-checkpoint-{}.db", std::process::id()));
        let path = CString::new(fixture.to_str().unwrap()).unwrap();
        {
            let setup = open(&path);
            exec(
                &setup,
                &CString::new(include_str!("../../../../schema.sql")).unwrap(),
            );
            exec(&setup, c"INSERT INTO users(id,username) VALUES(1,'alice');");
        }
        let mut db = Db::open(fixture.to_str().unwrap(), true);
        let mut out = Vec::new();
        assert_eq!(db.create(1, "alice", "before snapshot", &mut out), 201);
        assert!(db.commit());
        let reader = open(&path);
        exec(&reader, c"BEGIN; SELECT * FROM posts;");
        for _ in 0..500 {
            out.clear();
            assert_eq!(db.create(1, "alice", &"x".repeat(500), &mut out), 201);
            assert!(db.commit());
        }
        assert!(db.wal_frames.load(Ordering::Relaxed) >= 1000);
        let (log, backfilled) = wait_for(&db, |log, backfilled| backfilled > 0 && backfilled < log);
        assert!(
            backfilled < log,
            "reader snapshot must pin newer WAL frames"
        );
        drop(reader);
        wait_for(&db, |log, backfilled| log > 0 && log == backfilled);
        let last = serde_json::from_slice::<serde_json::Value>(&out).unwrap()["post"]["id"]
            .as_i64()
            .unwrap();
        drop(db); // Joins the checkpointer before closing the request connection.
        let mut reopened = Db::open(fixture.to_str().unwrap(), true);
        out.clear();
        assert_eq!(reopened.read(Some(last), &mut out), 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&out).unwrap()["post"]["body"],
            "x".repeat(500)
        );
        drop(reopened);
        std::fs::remove_file(fixture).unwrap();
    }
}
