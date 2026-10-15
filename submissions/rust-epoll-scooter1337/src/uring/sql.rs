//! One SQL owner. Ring threads never wait for its queries, COMMIT or checkpoint I/O.
use crate::{App, Frame, error};
use std::{
    borrow::Cow,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Instant,
};

const MAX_PENDING: usize = 65536;
const BATCH: usize = 32;

pub(super) struct Request {
    bytes: Vec<u8>,
    method_end: usize,
    path_end: usize,
    auth_end: Option<usize>,
    trace_id: u64,
    close: bool,
}
impl Request {
    pub(super) fn copy(frame: &Frame<'_>) -> Self {
        let mut bytes = Vec::with_capacity(
            frame.method.len()
                + frame.path.len()
                + frame.authorization.map_or(0, str::len)
                + frame.body.len(),
        );
        bytes.extend_from_slice(frame.method.as_bytes());
        let method_end = bytes.len();
        bytes.extend_from_slice(frame.path.as_bytes());
        let path_end = bytes.len();
        let auth_end = frame.authorization.map(|auth| {
            bytes.extend_from_slice(auth.as_bytes());
            bytes.len()
        });
        bytes.extend_from_slice(&frame.body);
        Self {
            bytes,
            method_end,
            path_end,
            auth_end,
            trace_id: frame.trace_id,
            close: frame.close,
        }
    }
    fn frame(&self) -> Frame<'_> {
        // The HTTP parser already supplied these fields as valid UTF-8 strings.
        let text = |s| std::str::from_utf8(s).expect("parsed HTTP field");
        Frame {
            method: text(&self.bytes[..self.method_end]),
            path: text(&self.bytes[self.method_end..self.path_end]),
            authorization: self
                .auth_end
                .map(|end| text(&self.bytes[self.path_end..end])),
            body: Cow::Borrowed(&self.bytes[self.auth_end.unwrap_or(self.path_end)..]),
            consumed: 0,
            trace_id: self.trace_id,
            close: self.close,
        }
    }
}

pub(super) struct Job {
    pub ring: usize,
    pub fd: usize,
    pub generation: u32,
    pub request: Request,
    pub batch: u64,
    pub queued: Instant,
}
pub(super) struct Reply {
    pub fd: usize,
    pub generation: u32,
    pub status: u16,
    pub body: Vec<u8>,
    pub trace: [u64; 6],
}
struct Completed {
    sender: mpsc::Sender<Reply>,
    wake: Arc<OwnedFd>,
}
pub(super) struct Port {
    pub id: usize,
    sender: mpsc::Sender<Job>,
    replies: mpsc::Receiver<Reply>,
    pub wake: Arc<OwnedFd>,
    pending: Arc<AtomicUsize>,
    peers: Arc<Vec<Arc<OwnedFd>>>,
}
impl Port {
    pub(super) fn submit(&self, make: impl FnOnce() -> Job) -> bool {
        // Count jobs until the ring consumes their replies, bounding queued,
        // executing AND completed response ownership across every ring.
        let mut n = self.pending.load(Ordering::Acquire);
        loop {
            if n >= MAX_PENDING {
                return false;
            }
            match self
                .pending
                .compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => n = current,
            }
        }
        self.sender.send(make()).expect("SQL worker exited");
        true
    }
    pub(super) fn reply(&self) -> Option<Reply> {
        match self.replies.try_recv() {
            Ok(reply) => {
                if self.pending.fetch_sub(1, Ordering::AcqRel) == MAX_PENDING {
                    // A ring with no outstanding jobs can still be waiting for
                    // capacity consumed by another ring. Wake it when full→free.
                    for fd in self.peers.iter() {
                        notify(fd);
                    }
                }
                Some(reply)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => panic!("SQL worker exited"),
        }
    }
    pub(super) fn has_capacity(&self) -> bool {
        self.pending.load(Ordering::Acquire) < MAX_PENDING
    }
    pub(super) fn drain_wake(&self) {
        let mut value = 0u64;
        loop {
            // SAFETY: owned nonblocking eventfd, writable eight-byte destination.
            let n =
                unsafe { libc::read(self.wake.as_raw_fd(), (&mut value as *mut u64).cast(), 8) };
            if n == 8 {
                return;
            }
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            assert_eq!(e.kind(), io::ErrorKind::WouldBlock, "eventfd read: {e}");
            return;
        }
    }
}
fn notify(fd: &OwnedFd) {
    let value = 1u64;
    loop {
        // SAFETY: owned eventfd and readable eight-byte source. EAGAIN means
        // the counter is already readable, so a wake is already pending.
        let n = unsafe { libc::write(fd.as_raw_fd(), (&value as *const u64).cast(), 8) };
        if n == 8 {
            return;
        }
        let e = io::Error::last_os_error();
        if e.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        assert_eq!(e.kind(), io::ErrorKind::WouldBlock, "eventfd write: {e}");
        return;
    }
}
impl Port {
    pub(super) fn notify(&self) {
        notify(&self.wake);
    }
}

pub(super) fn start(app: Arc<Mutex<App>>, rings: usize) -> io::Result<Vec<Port>> {
    let (sender, jobs) = mpsc::channel::<Job>();
    let pending = Arc::new(AtomicUsize::new(0));
    let mut ports = Vec::new();
    let mut completed = Vec::new();
    for id in 0..rings {
        // SAFETY: eventfd has no pointer arguments and returns a new owned FD.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful eventfd creation transferred this unique descriptor.
        let wake = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
        let (send, replies) = mpsc::channel();
        completed.push(Completed {
            sender: send,
            wake: Arc::clone(&wake),
        });
        ports.push(Port {
            id,
            sender: sender.clone(),
            replies,
            wake,
            pending: Arc::clone(&pending),
            peers: Arc::new(Vec::new()),
        });
    }
    let peers = Arc::new(completed.iter().map(|c| Arc::clone(&c.wake)).collect());
    for port in &mut ports {
        port.peers = Arc::clone(&peers);
    }
    std::thread::Builder::new().name("sqlite-requests".into()).spawn(move || {
        let mut batch = Vec::with_capacity(BATCH);
        let mut replies = Vec::with_capacity(BATCH);
        let mut second = Instant::now();
        let (mut count, mut queue_max, mut commit_max) = (0u64, 0u128, 0u128);
        while let Ok(first) = jobs.recv() {
            batch.push(first);
            batch.extend(jobs.try_iter().take(BATCH - 1));
            // Only this thread uses the request connection. The existing mutex
            // preserves the App's SQLite Send invariant and supports tests that
            // deliberately stall SQL without stalling transport completion work.
            let mut app = app.lock().unwrap();
            let batch_activity = app.db.batch();
            for job in &batch {
                queue_max = queue_max.max(job.queued.elapsed().as_micros());
                let mut body = Vec::with_capacity(512);
                let started = if job.request.trace_id != 0 { crate::latency_trace::now() } else { 0 };
                let status = app.serve(&job.request.frame(), &mut body);
                let finished = if job.request.trace_id != 0 { crate::latency_trace::now() } else { 0 };
                replies.push(Reply { fd: job.fd, generation: job.generation, status, body, trace: [job.request.trace_id, job.batch, started, finished, 0, 0] });
            }
            let traced = batch.iter().any(|j| j.request.trace_id != 0);
            let commit_begin = if traced { crate::latency_trace::now() } else { 0 };
            let start = Instant::now();
            let committed = app.db.commit();
            commit_max = commit_max.max(start.elapsed().as_micros());
            let commit_end = if traced { crate::latency_trace::now() } else { 0 };
            drop(batch_activity);
            drop(app);
            let mut wake = 0u64;
            for (job, mut reply) in batch.drain(..).zip(replies.drain(..)) {
                // Db::commit rolls back on failure before any reply is released.
                if !committed { reply.body.clear(); reply.status = error(&mut reply.body, 500, "internal server error"); }
                reply.trace[4] = commit_begin; reply.trace[5] = commit_end;
                completed[job.ring].sender.send(reply).expect("ring exited");
                wake |= 1 << job.ring;
                count += 1;
            }
            for (id, completed) in completed.iter().enumerate() {
                if wake & (1 << id) != 0 { notify(&completed.wake); }
            }
            if second.elapsed().as_secs() >= 1 {
                eprintln!("SQL_WORKER requests={count} pending={} queue_max_us={queue_max} commit_max_us={commit_max}", pending.load(Ordering::Relaxed));
                queue_max = 0; commit_max = 0; second = Instant::now();
            }
        }
    })?;
    Ok(ports)
}
