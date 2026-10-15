use std::{
    cell::RefCell,
    fs::File,
    io::{BufWriter, Write},
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
thread_local! { static EVENTS: RefCell<Vec<[u64;5]>> = const { RefCell::new(Vec::new()) }; }
static ENABLED: OnceLock<bool> = OnceLock::new();
static DUMP: AtomicBool = AtomicBool::new(false);
static LOST: AtomicU64 = AtomicU64::new(0);
extern "C" fn request_dump(_: libc::c_int) {
    DUMP.store(true, Ordering::Relaxed);
}
pub fn init() {
    if enabled() {
        // SAFETY: signal handler only stores to a lock-free atomic; it performs
        // no allocation, locking or I/O. Actual dumping happens on the ring thread.
        unsafe {
            libc::signal(libc::SIGUSR2, request_dump as *const () as usize);
        }
    }
}
fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var_os("SERVER_TRACE_FILE").is_some())
}
pub fn now() -> u64 {
    if enabled() {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    } else {
        0
    }
}
pub fn at(ns: u64, id: u64, kind: u64, a: u64, b: u64) {
    if !enabled() {
        return;
    }
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        if events.len() < 2_000_000 {
            events.push([ns, id, kind, a, b]);
        } else {
            LOST.fetch_add(1, Ordering::Relaxed);
        }
    });
}
pub fn event(id: u64, kind: u64, a: u64, b: u64) {
    at(now(), id, kind, a, b);
}
pub fn maybe_dump() {
    if !DUMP.swap(false, Ordering::Relaxed) {
        return;
    }
    let mut out =
        BufWriter::new(File::create(std::env::var_os("SERVER_TRACE_FILE").unwrap()).unwrap());
    EVENTS.with(|events| {
        for event in events.borrow().iter() {
            for value in event {
                out.write_all(&value.to_le_bytes()).unwrap();
            }
        }
        eprintln!(
            "latency trace: {} events, {} lost",
            events.borrow().len(),
            LOST.load(Ordering::Relaxed)
        );
    });
    out.flush().unwrap();
}
