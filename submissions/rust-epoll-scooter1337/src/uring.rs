//! Completion-based Linux I/O. Fixed descriptors belong to each ring, rather than
//! the process file table. SQLite remains serialized behind one batch mutex.
mod sql;
use crate::{App, Parsed, error, frame};
use io_uring::{IoUring, cqueue, opcode, squeue, types};
use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error, realloc},
    collections::VecDeque,
    io,
    net::TcpListener,
    os::fd::AsRawFd,
    ptr::{self, NonNull},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

const MAX_WORKERS: usize = 8;
const TARGET_SLOTS: usize = MAX_WORKERS * 65535;
const NBUFS: usize = 4096;
const BUFSIZE: usize = 2048;
const INPUT_LIMIT: usize = 1_114_112;
const OUTPUT_HIGH: usize = 65536;
const GROUP: u16 = 0;
const ACCEPT: u64 = 1;
const RECV: u64 = 2;
const SEND: u64 = 3;
const IGNORE: u64 = 4;
const SHUTDOWN: u64 = 5;
const SQL_READY: u64 = 6;
const LIVE: u32 = 1;
const RECEIVING: u32 = 2;
const SENDING: u32 = 4;
const CLOSING: u32 = 8;
const SHUT: u32 = 16;
const PAUSED: u32 = 32;
const DIRTY: u32 = 64;
const CONTINUED: u32 = 128;
const READY: u32 = 256;
const SHUT_PENDING: u32 = 512;
const REARM: u32 = 1024;
const SQL_PENDING: u32 = 2048;
const SQL_WAIT: u32 = 4096;
const PEER_EOF: u32 = 8192;
const INDEX_SHIFT: u32 = 14;
const FLAG_MASK: u32 = (1 << INDEX_SHIFT) - 1;

#[derive(Clone, Copy)]
pub struct Tuning {
    pub batch: usize,
    pub wait_us: u32,
}
impl Default for Tuning {
    fn default() -> Self {
        Self {
            batch: 32,
            wait_us: 25,
        }
    }
}

/// A compact owning buffer. Its allocation never moves while a send is in flight.
/// Keeping lengths as u32 makes a complete connection fit one 64-byte cache line.
#[repr(C)]
#[derive(Default)]
struct Buffer {
    ptr: *mut u8,
    len: u32,
    cap: u32,
}
impl Buffer {
    fn bytes(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // reserve initializes exactly the written prefix before increasing len.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
    fn reserve(&mut self, needed: usize, minimum: usize) {
        if needed <= self.cap as usize {
            return;
        }
        assert!(needed <= 4 * 1024 * 1024);
        let cap = needed.max(minimum).next_power_of_two();
        let layout = Layout::from_size_align(cap, 8).unwrap();
        // Both allocation paths use the same alignment and allocator. No caller
        // may grow a Buffer after it has been transferred into Client::flight.
        let p = unsafe {
            if self.cap == 0 {
                alloc(layout)
            } else {
                realloc(
                    self.ptr,
                    Layout::from_size_align(self.cap as usize, 8).unwrap(),
                    cap,
                )
            }
        };
        if p.is_null() {
            handle_alloc_error(layout);
        }
        self.ptr = p;
        self.cap = cap as u32;
    }
    fn extend(&mut self, bytes: &[u8]) {
        self.extend_with_floor(bytes, 4096);
    }
    fn extend_input(&mut self, bytes: &[u8]) {
        self.extend_with_floor(bytes, 256);
    }
    fn extend_with_floor(&mut self, bytes: &[u8], minimum: usize) {
        let old = self.len as usize;
        self.reserve(old + bytes.len(), minimum);
        if !bytes.is_empty() {
            // Source and destination are distinct allocations (or stack bytes).
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), self.ptr.add(old), bytes.len());
            }
        }
        self.len = (old + bytes.len()) as u32;
    }
    fn consume(&mut self, n: usize) {
        assert!(n <= self.len as usize);
        let left = self.len as usize - n;
        if left != 0 {
            // Moving a partial/pipelined tail can overlap within the allocation.
            unsafe {
                ptr::copy(self.ptr.add(n), self.ptr, left);
            }
        }
        self.len = left as u32;
    }
}
impl Drop for Buffer {
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                dealloc(
                    self.ptr,
                    Layout::from_size_align(self.cap as usize, 8).unwrap(),
                );
            }
        }
    }
}
#[repr(C)]
#[derive(Default)]
struct Client {
    input: Buffer,
    output: Buffer,
    flight: Buffer,
    sent: u32,
    generation: u32,
    touched: u32,
    state: u32,
}
const _: () = assert!(std::mem::size_of::<Client>() == 64);

struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}
impl Mapping {
    fn new(len: usize) -> io::Result<Self> {
        let p = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(p.cast()).unwrap(),
            len,
        })
    }
}
impl Drop for Mapping {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}
struct Pending {
    fd: usize,
    generation: u32,
    start: u32,
    count: u32,
    interim: bool,
}
struct Listener {
    fd: i32,
    accepting: bool,
    generation: u32,
}
struct Worker {
    // Destroy the ring before any of the allocations referenced by its entries.
    ring: IoUring,
    recv_memory: Mapping,
    buf_ring: Option<Mapping>,
    tail: u16,
    returned: Vec<u16>,
    rearm: VecDeque<(usize, u32)>,
    clients: Vec<Client>,
    slots: usize,
    reserve: usize,
    listeners: Vec<Listener>,
    open: usize,
    ready: VecDeque<(usize, u32)>,
    completions: Vec<(u64, i32, u32)>,
    pending: Vec<Pending>,
    pool: Vec<Buffer>,
    body: Vec<u8>,
    start: Instant,
    now: u32,
    tuning: Tuning,
    trace_ids: std::collections::HashMap<usize, u64>,
    trace_batch: u64,
    trace_active: bool,
    sql: Option<sql::Port>,
    sql_waiters: VecDeque<(usize, u32)>,
    input_cap_bytes: usize,
}
fn tag(op: u64, fd: usize, generation: u32) -> u64 {
    op | ((fd as u64) << 8) | ((generation as u64) << 32)
}
impl Worker {
    fn new(listeners: Vec<i32>, slots: usize, tuning: Tuning) -> io::Result<Self> {
        let ring = IoUring::builder()
            .setup_cqsize(16384)
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(4096)?;
        ring.submitter().register_files_sparse(slots as u32)?;
        let recv_memory = Mapping::new(NBUFS * BUFSIZE)?;
        let descriptors = Mapping::new(NBUFS * std::mem::size_of::<types::BufRingEntry>())?;
        // mmap supplies page alignment. Both mappings outlive every receive and
        // are private to this issuer; the kernel owns each published buffer until
        // its completion transfers it back to us.
        let registered = unsafe {
            ring.submitter().register_buf_ring_with_flags(
                descriptors.ptr.as_ptr() as u64,
                NBUFS as u16,
                GROUP,
                0,
            )
        }
        .is_ok();
        let mut w = Self {
            ring,
            recv_memory,
            buf_ring: registered.then_some(descriptors),
            tail: 0,
            returned: Vec::with_capacity(NBUFS),
            rearm: VecDeque::new(),
            clients: Vec::new(),
            slots,
            reserve: 1024.min(slots / 8),
            listeners: listeners
                .into_iter()
                .map(|fd| Listener {
                    fd,
                    accepting: false,
                    generation: 0,
                })
                .collect(),
            open: 0,
            ready: VecDeque::new(),
            completions: Vec::with_capacity(16384),
            pending: Vec::with_capacity(1024),
            pool: Vec::with_capacity(256),
            body: Vec::with_capacity(16384),
            start: Instant::now(),
            now: 0,
            tuning,
            trace_ids: std::collections::HashMap::new(),
            trace_batch: 0,
            trace_active: false,
            sql: None,
            sql_waiters: VecDeque::new(),
            input_cap_bytes: 0,
        };
        if w.buf_ring.is_some() {
            for bid in 0..NBUFS {
                w.recycle(bid as u16);
            }
            w.publish_buffers();
        } else {
            w.queue(
                opcode::ProvideBuffers::new(
                    w.recv_memory.ptr.as_ptr(),
                    BUFSIZE as i32,
                    NBUFS as u16,
                    GROUP,
                    0,
                )
                .build()
                .user_data(IGNORE),
            );
            w.ring.submit_and_wait(1)?;
            let result = w
                .ring
                .completion()
                .next()
                .ok_or_else(|| io::Error::other("missing provided-buffer completion"))?
                .result();
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
        }
        Ok(w)
    }
    fn queue(&mut self, entry: squeue::Entry) {
        loop {
            // Every pointer in an SQE refers to a mapping or a flight Buffer
            // retained until its final CQE. The SQE itself is copied into the SQ.
            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                break;
            }
            // Return completed buffers before submitting any rearmed receives.
            // The legacy provider queues SQEs itself, so it is published by the
            // outer completion loop rather than recursively from here.
            if self.buf_ring.is_some() {
                self.publish_buffers();
            }
            self.ring.submit().expect("submit full I/O queue");
        }
    }
    fn accept(&mut self) {
        for index in 0..self.listeners.len() {
            let listener = &mut self.listeners[index];
            if listener.accepting {
                continue;
            }
            listener.generation = listener.generation.wrapping_add(1);
            let e = opcode::AcceptMulti::new(types::Fd(listener.fd))
                .allocate_file_index(true)
                .build()
                .user_data(tag(ACCEPT, index, listener.generation));
            listener.accepting = true;
            self.queue(e);
        }
    }
    fn stop_accept(&mut self) {
        for index in 0..self.listeners.len() {
            let listener = &mut self.listeners[index];
            if !listener.accepting {
                continue;
            }
            let e = opcode::AsyncCancel::new(tag(ACCEPT, index, listener.generation))
                .build()
                .user_data(IGNORE)
                .flags(squeue::Flags::SKIP_SUCCESS);
            listener.accepting = false;
            self.queue(e);
        }
    }
    fn recv(&mut self, fd: usize) {
        if self.clients[fd].state & PEER_EOF != 0 {
            return;
        }
        let e = opcode::RecvMulti::new(types::Fixed(fd as u32), GROUP)
            .build()
            .user_data(tag(RECV, fd, self.clients[fd].generation));
        self.queue(e);
        self.clients[fd].state = (self.clients[fd].state | RECEIVING) & !REARM;
    }
    fn send(&mut self, fd: usize) {
        if let Some(&id) = self.trace_ids.get(&fd) {
            self.trace_active = true;
            crate::latency_trace::event(id, 5, self.trace_batch, 0);
        }
        let c = &mut self.clients[fd];
        assert!(c.sent < c.flight.len);
        let e = opcode::Send::new(
            types::Fixed(fd as u32),
            unsafe { c.flight.ptr.add(c.sent as usize) },
            c.flight.len - c.sent,
        )
        .flags(libc::MSG_NOSIGNAL)
        .build()
        .user_data(tag(SEND, fd, c.generation));
        c.state |= SENDING;
        self.queue(e);
    }
    fn recycle(&mut self, bid: u16) {
        assert!((bid as usize) < NBUFS);
        if let Some(ref descriptors) = self.buf_ring {
            let index = self.tail as usize & (NBUFS - 1);
            // This descriptor slot is outside the kernel's unconsumed interval.
            // Only a completed receive's buffer is re-published into the ring.
            let entry = unsafe {
                &mut *descriptors
                    .ptr
                    .as_ptr()
                    .cast::<types::BufRingEntry>()
                    .add(index)
            };
            entry.set_addr(
                unsafe { self.recv_memory.ptr.as_ptr().add(bid as usize * BUFSIZE) } as u64,
            );
            entry.set_len(BUFSIZE as u32);
            entry.set_bid(bid);
            self.tail = self.tail.wrapping_add(1);
        } else {
            self.returned.push(bid);
        }
    }
    fn publish_buffers(&mut self) {
        if let Some(ref descriptors) = self.buf_ring {
            // Release publishes initialized descriptors after all reads of their
            // old contents have finished. The u16 tail is naturally aligned.
            let tail = unsafe { types::BufRingEntry::tail(descriptors.ptr.as_ptr().cast()) };
            unsafe {
                (&*tail.cast::<AtomicU16>()).store(self.tail, Ordering::Release);
            }
        } else {
            let mut i = 0;
            while i < self.returned.len() {
                let first = self.returned[i];
                let mut n = 1;
                while i + n < self.returned.len()
                    && self.returned[i + n] as usize == first as usize + n
                {
                    n += 1;
                }
                let p = unsafe { self.recv_memory.ptr.as_ptr().add(first as usize * BUFSIZE) };
                self.queue(
                    opcode::ProvideBuffers::new(p, BUFSIZE as i32, n as u16, GROUP, first)
                        .build()
                        .user_data(IGNORE)
                        .flags(squeue::Flags::SKIP_SUCCESS),
                );
                i += n;
            }
            self.returned.clear();
        }
    }
    fn dirty(&mut self, fd: usize) -> usize {
        let c = &mut self.clients[fd];
        if c.state & DIRTY != 0 {
            return (c.state >> INDEX_SHIFT) as usize;
        }
        let index = self.pending.len();
        assert!(index < 1 << (32 - INDEX_SHIFT));
        self.pending.push(Pending {
            fd,
            generation: c.generation,
            start: c.output.len,
            count: 0,
            interim: false,
        });
        c.state |= DIRTY | ((index as u32) << INDEX_SHIFT);
        index
    }
    fn output(&mut self, fd: usize) {
        if self.clients[fd].output.cap == 0 {
            self.clients[fd].output = self.pool.pop().unwrap_or_default();
        }
    }
    fn retire(&mut self, mut b: Buffer) {
        b.len = 0;
        if b.cap <= 16384 && self.pool.len() < 256 {
            self.pool.push(b);
        }
    }
    fn schedule(&mut self, fd: usize) {
        let c = &mut self.clients[fd];
        if c.state & READY == 0 {
            c.state |= READY;
            self.ready.push_back((fd, c.generation));
        }
    }
    fn pause(&mut self, fd: usize) {
        let c = &mut self.clients[fd];
        if c.state & PAUSED != 0 {
            return;
        }
        c.state |= PAUSED;
        if c.state & RECEIVING != 0 {
            let e = opcode::AsyncCancel::new(tag(RECV, fd, c.generation))
                .build()
                .user_data(IGNORE)
                .flags(squeue::Flags::SKIP_SUCCESS);
            self.queue(e);
        }
    }
    fn shutdown(&mut self, fd: usize) {
        let c = &mut self.clients[fd];
        if c.state & SHUT != 0 {
            return;
        }
        c.state |= SHUT | SHUT_PENDING;
        // Shutdown runs in io-wq and resolves its fixed slot there. Keep that
        // slot occupied until its CQE, even if recv reaches EOF first.
        let entry = opcode::Shutdown::new(types::Fixed(fd as u32), libc::SHUT_RDWR)
            .build()
            .user_data(tag(SHUTDOWN, fd, c.generation));
        self.queue(entry);
    }
    fn on_shutdown(&mut self, fd: usize, generation: u32, result: i32) {
        if fd >= self.clients.len()
            || self.clients[fd].generation != generation
            || self.clients[fd].state & LIVE == 0
        {
            return;
        }
        self.clients[fd].state &= !SHUT_PENDING;
        if result < 0 && self.clients[fd].state & RECEIVING != 0 {
            self.queue(
                opcode::AsyncCancel::new(tag(RECV, fd, generation))
                    .build()
                    .user_data(IGNORE)
                    .flags(squeue::Flags::SKIP_SUCCESS),
            );
        }
        self.close(fd);
    }
    fn close(&mut self, fd: usize) {
        let c = &mut self.clients[fd];
        if c.state & LIVE == 0
            || c.state & (SENDING | SHUT_PENDING | SQL_PENDING) != 0
            || c.output.len != 0
        {
            return;
        }
        if c.state & RECEIVING != 0 {
            self.shutdown(fd);
            return;
        }
        self.queue(
            opcode::Close::new(types::Fixed(fd as u32))
                .build()
                .user_data(IGNORE)
                .flags(squeue::Flags::SKIP_SUCCESS),
        );
        let c = &mut self.clients[fd];
        let generation = c.generation.wrapping_add(1);
        self.input_cap_bytes -= c.input.cap as usize;
        *c = Client {
            generation,
            ..Default::default()
        };
        self.open -= 1;
    }
    fn process(&mut self, fd: usize, bytes: &[u8]) {
        let process_ns = crate::latency_trace::now();
        let mut saved = std::mem::take(&mut self.clients[fd].input);
        self.input_cap_bytes -= saved.cap as usize;
        if self.clients[fd].state & (PAUSED | SQL_PENDING | SQL_WAIT) != 0 {
            if saved.len as usize + bytes.len() <= INPUT_LIMIT {
                saved.extend_input(bytes);
            } else {
                self.clients[fd].state |= CLOSING;
            }
            self.input_cap_bytes += saved.cap as usize;
            self.clients[fd].input = saved;
            return;
        }
        let input = if saved.len == 0 {
            bytes
        } else {
            if saved.len as usize + bytes.len() <= INPUT_LIMIT {
                saved.extend_input(bytes);
            } else {
                self.clients[fd].state |= CLOSING;
                self.close(fd);
                return;
            }
            saved.bytes()
        };
        let mut offset = 0;
        let mut closing = false;
        if offset < input.len() {
            match frame(&input[offset..]) {
                Ok(Parsed::Complete(req)) => {
                    self.clients[fd].state &= !CONTINUED;
                    self.body.clear();
                    if req.trace_id != 0 {
                        self.trace_active = true;
                        self.trace_ids.insert(fd, req.trace_id);
                        crate::latency_trace::at(
                            process_ns,
                            req.trace_id,
                            1,
                            self.trace_batch,
                            fd as u64,
                        );
                    }
                    let port = self.sql.as_ref().expect("SQL port");
                    if !port.submit(|| sql::Job {
                        ring: port.id,
                        fd,
                        generation: self.clients[fd].generation,
                        request: sql::Request::copy(&req),
                        batch: self.trace_batch,
                        queued: Instant::now(),
                    }) {
                        // Backpressure without rejecting a request or retaining a
                        // provided receive buffer. Resume only after SQL progress.
                        self.clients[fd].state |= SQL_WAIT;
                        self.sql_waiters
                            .push_back((fd, self.clients[fd].generation));
                    } else {
                        self.clients[fd].state |= SQL_PENDING;
                        offset += req.consumed;
                        closing = req.close;
                    }
                    // One outstanding SQL request per connection preserves HTTP
                    // pipeline order without adding idle per-connection storage.
                }
                Ok(Parsed::Incomplete { expect_continue }) => {
                    if self.clients[fd].state & PEER_EOF != 0 {
                        closing = true;
                    } else if expect_continue && self.clients[fd].state & CONTINUED == 0 {
                        let index = self.dirty(fd);
                        self.output(fd);
                        self.clients[fd]
                            .output
                            .extend(b"HTTP/1.1 100 Continue\r\n\r\n");
                        self.pending[index].interim = true;
                        self.clients[fd].state |= CONTINUED;
                    }
                }
                Err(()) => {
                    self.body.clear();
                    error(&mut self.body, 400, "malformed request");
                    let index = self.dirty(fd);
                    self.output(fd);
                    response(400, &self.body, true, &mut self.clients[fd].output);
                    self.pending[index].count += 1;
                    closing = true;
                }
            }
        }
        if !closing && offset < input.len() {
            if saved.len == 0 {
                saved.extend_input(&bytes[offset..]);
            } else {
                saved.consume(offset);
            }
            self.input_cap_bytes += saved.cap as usize;
            self.clients[fd].input = saved;
        } else if saved.cap != 0 {
            self.retire(saved);
        }
        if closing {
            self.dirty(fd);
            self.clients[fd].state |= CLOSING;
        }
        if self.clients[fd].output.len as usize >= OUTPUT_HIGH {
            self.pause(fd);
        }
    }
    fn on_accept(&mut self, index: usize, generation: u32, result: i32, flags: u32) {
        let listener = &mut self.listeners[index];
        if !cqueue::more(flags) && generation == listener.generation {
            listener.accepting = false;
        }
        if result < 0 {
            return;
        }
        let fd = result as usize;
        assert!(fd < self.slots);
        if self.clients.len() <= fd {
            self.clients.resize_with(
                (fd + 1).next_multiple_of(1024).min(self.slots),
                Client::default,
            );
        }
        let generation = self.clients[fd].generation;
        assert!(self.clients[fd].state & LIVE == 0);
        self.clients[fd] = Client {
            generation,
            touched: self.now,
            state: LIVE,
            ..Default::default()
        };
        self.open += 1;
        self.recv(fd);
        if self.slots - self.open < self.reserve {
            self.stop_accept();
        }
    }
    fn on_recv(&mut self, fd: usize, generation: u32, result: i32, flags: u32) {
        let selected = cqueue::buffer_select(flags);
        let valid = fd < self.clients.len()
            && self.clients[fd].generation == generation
            && self.clients[fd].state & LIVE != 0;
        if valid {
            if !cqueue::more(flags) {
                self.clients[fd].state &= !RECEIVING;
            }
            if result > 0 {
                let bid = selected.expect("receive buffer id");
                assert!((bid as usize) < NBUFS && result as usize <= BUFSIZE);
                self.clients[fd].touched = self.now;
                if self.clients[fd].state & CLOSING == 0 {
                    // CQE transfers this buffer to the issuer. It is not returned
                    // to the kernel until process finishes borrowing its bytes.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(
                            self.recv_memory.ptr.as_ptr().add(bid as usize * BUFSIZE),
                            result as usize,
                        )
                    };
                    self.process(fd, bytes);
                }
            } else if result == 0 {
                self.clients[fd].state |= PEER_EOF;
                if self.clients[fd].state & CLOSING != 0 {
                    self.close(fd);
                } else if self.clients[fd].state & (SQL_PENDING | SQL_WAIT) == 0 {
                    if self.clients[fd].input.len != 0 {
                        self.schedule(fd);
                    } else {
                        self.clients[fd].state |= CLOSING;
                        self.close(fd);
                    }
                }
            } else if (result != -libc::ENOBUFS && result != -libc::ECANCELED)
                || self.clients[fd].state & CLOSING != 0
            {
                self.clients[fd].state |= CLOSING;
                self.close(fd);
            }
            let c = &self.clients[fd];
            if c.state & (RECEIVING | CLOSING | PAUSED | REARM) == 0 && c.state & LIVE != 0 {
                if result == -libc::ENOBUFS {
                    // A depleted receive joins a bounded retry pass after buffers
                    // and responses have been returned. Retrying every failure
                    // during CQ processing can generate another failure storm.
                    self.clients[fd].state |= REARM;
                    self.rearm.push_back((fd, generation));
                } else {
                    self.recv(fd);
                }
            }
        }
        if let Some(bid) = selected {
            self.recycle(bid);
        }
    }
    fn on_send(&mut self, fd: usize, generation: u32, result: i32) {
        if fd >= self.clients.len()
            || self.clients[fd].generation != generation
            || self.clients[fd].state & LIVE == 0
        {
            return;
        }
        if let Some(&id) = self.trace_ids.get(&fd) {
            self.trace_active = true;
            crate::latency_trace::event(id, 6, self.trace_batch, result as u64);
        }
        let c = &mut self.clients[fd];
        c.state &= !SENDING;
        if result == -libc::EAGAIN || result == -libc::EINTR {
            self.send(fd);
            return;
        }
        c.touched = self.now;
        if result > 0 {
            c.sent += result as u32;
            assert!(c.sent <= c.flight.len);
        } else {
            c.state |= CLOSING;
            c.output.len = 0;
            c.sent = c.flight.len;
        }
        if c.sent < c.flight.len {
            self.send(fd);
            return;
        }
        self.trace_ids.remove(&fd);
        c.sent = 0;
        let done = std::mem::take(&mut c.flight);
        self.retire(done);
        self.dirty(fd);
        if self.clients[fd].state & PAUSED != 0
            && (self.clients[fd].output.len as usize) < OUTPUT_HIGH / 2
        {
            self.clients[fd].state &= !PAUSED;
            if self.clients[fd].input.len != 0 {
                self.schedule(fd);
            }
            if self.clients[fd].state & (RECEIVING | CLOSING) == 0 {
                self.recv(fd);
            }
        }
    }
    fn flush(&mut self, committed: bool) {
        if !committed {
            self.body.clear();
            error(&mut self.body, 500, "internal server error");
            for p in &self.pending {
                let c = &mut self.clients[p.fd];
                if c.generation != p.generation || c.state & LIVE == 0 {
                    continue;
                }
                c.output.len = p.start;
                for i in 0..p.count {
                    response(
                        500,
                        &self.body,
                        c.state & CLOSING != 0 && i + 1 == p.count,
                        &mut c.output,
                    );
                }
                if p.interim {
                    c.output.extend(b"HTTP/1.1 100 Continue\r\n\r\n");
                }
            }
        }
        for i in 0..self.pending.len() {
            let fd = self.pending[i].fd;
            let c = &mut self.clients[fd];
            if c.generation != self.pending[i].generation {
                continue;
            }
            c.state &= FLAG_MASK & !DIRTY;
            if c.state & LIVE == 0 || c.state & SENDING != 0 {
                continue;
            }
            if c.output.len != 0 {
                c.flight = std::mem::take(&mut c.output);
                c.sent = 0;
                self.send(fd);
            } else if c.state & CLOSING != 0 {
                self.close(fd);
            }
        }
        self.pending.clear();
    }
    fn arm_sql(&mut self) {
        let fd = self.sql.as_ref().unwrap().wake.as_raw_fd();
        self.queue(
            opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as u32)
                .build()
                .user_data(SQL_READY),
        );
    }
    fn sql_ready(&mut self, result: i32) {
        assert!(result >= 0, "SQL wake: {result}");
        self.sql.as_ref().unwrap().drain_wake();
        let mut count = 0;
        while count < 1024 {
            let Some(reply) = self.sql.as_ref().unwrap().reply() else {
                break;
            };
            count += 1;
            if reply.fd >= self.clients.len()
                || self.clients[reply.fd].generation != reply.generation
                || self.clients[reply.fd].state & LIVE == 0
            {
                continue;
            }
            let fd = reply.fd;
            self.clients[fd].state &= !SQL_PENDING;
            self.dirty(fd);
            self.output(fd);
            if self.clients[fd].state & PEER_EOF != 0 && self.clients[fd].input.len == 0 {
                self.clients[fd].state |= CLOSING;
            }
            let close = self.clients[fd].state & CLOSING != 0;
            if reply.trace[0] != 0 {
                let [id, batch, begin, end, commit_begin, commit_end] = reply.trace;
                crate::latency_trace::at(begin, id, 2, batch, 0);
                crate::latency_trace::at(end, id, 3, batch, reply.status as u64);
                // Per-request commit boundaries use new kinds16/17. These must
                // not masquerade as ring batch13/14 events from the old design.
                crate::latency_trace::at(commit_begin, id, 16, batch, 0);
                crate::latency_trace::at(commit_end, id, 17, batch, 0);
            }
            response(
                reply.status,
                &reply.body,
                close,
                &mut self.clients[fd].output,
            );
            if !close && self.clients[fd].input.len != 0 {
                self.schedule(fd);
            }
        }
        // If this fairness batch filled, wake again even if the writer has no
        // further work. A harmless extra wake avoids stranding completed replies.
        if count == 1024 {
            self.sql.as_ref().unwrap().notify();
        }
        if !self.sql_waiters.is_empty() {
            for _ in 0..self.sql_waiters.len().min(128) {
                let (fd, generation) = self.sql_waiters.pop_front().unwrap();
                if self.clients[fd].generation != generation {
                    continue;
                }
                self.clients[fd].state &= !SQL_WAIT;
                if self.clients[fd].state & LIVE != 0 {
                    self.schedule(fd);
                }
            }
        }
        if !self.sql_waiters.is_empty() && self.sql.as_ref().unwrap().has_capacity() {
            self.sql.as_ref().unwrap().notify();
        }
        self.arm_sql();
    }
    fn run(mut self, port: sql::Port) -> ! {
        self.sql = Some(port);
        self.arm_sql();
        let mut last_count = 0;
        let mut sweep = 0;
        // Diagnostic only: 0 restores the original complete CQ drain.
        let cq_limit = std::env::var("SCOOTER_CQ_LIMIT")
            .map(|s| s.parse::<usize>().expect("SCOOTER_CQ_LIMIT"))
            .unwrap_or(self.tuning.batch);
        let rearm_after_drain = std::env::var("SCOOTER_REARM_AFTER_DRAIN").as_deref() == Ok("1");
        let mut stats = [0u64; 8];
        let mut cq_peak = 0;
        let mut stats_second = 0;
        loop {
            self.trace_batch += 1;
            self.trace_active = false;
            let loop_ns = crate::latency_trace::now();
            let timeout = if last_count >= 4 || !self.rearm.is_empty() {
                types::Timespec::new().nsec(self.tuning.wait_us * 1000)
            } else {
                types::Timespec::new().sec(2)
            };
            let wait = if !self.ready.is_empty() {
                0
            } else if last_count >= 4 {
                self.tuning.batch.min(self.open.saturating_mul(2).max(1))
            } else {
                1
            };
            let result = self
                .ring
                .submitter()
                .submit_with_args(wait, &types::SubmitArgs::new().timespec(&timeout));
            if let Err(e) = result {
                if !matches!(
                    e.raw_os_error(),
                    Some(libc::ETIME | libc::EINTR | libc::EAGAIN | libc::EBUSY)
                ) {
                    eprintln!("io_uring: {e}");
                    std::process::exit(1);
                }
            }
            let submitted_ns = crate::latency_trace::now();
            self.now = self.start.elapsed().as_secs() as u32;
            let cq_len = self.ring.completion().len();
            cq_peak = cq_peak.max(cq_len);
            self.completions.clear();
            self.completions.extend(
                self.ring
                    .completion()
                    // Bound the work before the next commit and response flush.
                    .take(if cq_limit == 0 { usize::MAX } else { cq_limit })
                    .map(|c| (c.user_data(), c.result(), c.flags())),
            );
            last_count = self.completions.len();
            stats[0] += 1;
            stats[1] += last_count as u64;
            for &(tag, result, flags) in &self.completions {
                match tag & 255 {
                    ACCEPT if result >= 0 => stats[2] += 1,
                    RECV if result > 0 => stats[3] += 1,
                    RECV if result == -libc::ENOBUFS => stats[4] += 1,
                    RECV => stats[5] += 1,
                    SEND => stats[6] += 1,
                    _ => {}
                }
                stats[7] += u64::from(cqueue::buffer_select(flags).is_some());
            }
            if self.now > stats_second {
                stats_second = self.now;
                eprintln!(
                    "RING_STATS ns={} second={} open={} cq={} cq_peak={} rearm={} ready={} sq={} loops={} cqes={} accepts={} recv_ok={} enobufs={} recv_other={} sends={} selected={} sql_wait={} input_cap_bytes={}",
                    crate::latency_trace::now(),
                    self.now,
                    self.open,
                    cq_len,
                    cq_peak,
                    self.rearm.len(),
                    self.ready.len(),
                    self.ring.submission().len(),
                    stats[0],
                    stats[1],
                    stats[2],
                    stats[3],
                    stats[4],
                    stats[5],
                    stats[6],
                    stats[7],
                    self.sql_waiters.len(),
                    self.input_cap_bytes,
                );
                cq_peak = 0;
            }
            let collected_ns = crate::latency_trace::now();
            let ready = self.ready.len().min(128);
            for _ in 0..ready {
                let (fd, generation) = self.ready.pop_front().unwrap();
                if self.clients[fd].generation != generation {
                    continue;
                }
                self.clients[fd].state &= !READY;
                if self.clients[fd].state & (LIVE | CLOSING | PAUSED) == LIVE {
                    self.process(fd, &[]);
                }
            }
            for i in 0..self.completions.len() {
                let (tag, result, flags) = self.completions[i];
                let fd = ((tag >> 8) & 0xffffff) as usize;
                let generation = (tag >> 32) as u32;
                match tag & 255 {
                    ACCEPT => self.on_accept(fd, generation, result, flags),
                    RECV => self.on_recv(fd, generation, result, flags),
                    SEND => self.on_send(fd, generation, result),
                    SHUTDOWN => self.on_shutdown(fd, generation, result),
                    SQL_READY => self.sql_ready(result),
                    _ => {}
                }
            }
            self.publish_buffers();
            self.flush(true);
            let flushed_ns = crate::latency_trace::now();
            if self.trace_active {
                for (kind, ns) in [
                    (10, loop_ns),
                    (11, submitted_ns),
                    (12, collected_ns),
                    (15, flushed_ns),
                ] {
                    crate::latency_trace::at(ns, 0, kind, self.trace_batch, last_count as u64);
                }
            }
            crate::latency_trace::maybe_dump();
            if !self.rearm.is_empty() && (!rearm_after_drain || self.ring.completion().is_empty()) {
                let returned = self
                    .completions
                    .iter()
                    .filter(|(_, _, flags)| cqueue::buffer_select(*flags).is_some())
                    .count();
                // Bound retries by progress, with a small allowance to restart
                // an otherwise idle ring. Unvisited clients remain queued once.
                let budget = returned.max(32).min(NBUFS / 2);
                for _ in 0..budget {
                    let Some((fd, generation)) = self.rearm.pop_front() else {
                        break;
                    };
                    let c = &mut self.clients[fd];
                    if c.generation != generation || c.state & REARM == 0 {
                        continue;
                    }
                    c.state &= !REARM;
                    if c.state & (LIVE | RECEIVING | CLOSING | PAUSED) == LIVE {
                        self.recv(fd);
                    }
                }
            }
            if self.slots - self.open >= self.reserve * 2 {
                self.accept();
            }
            if self.now >= sweep + 10 {
                sweep = self.now;
                for fd in 0..self.clients.len() {
                    let c = &mut self.clients[fd];
                    // Idle keep-alives are retained; only stalled partial input
                    // or blocked output is closed after 120 seconds.
                    if c.state & LIVE != 0
                        && (c.input.len != 0 || c.flight.len != 0 || c.output.len != 0)
                        && self.now.saturating_sub(c.touched) >= 120
                    {
                        c.state |= CLOSING;
                        c.output.len = 0;
                        self.shutdown(fd);
                        self.close(fd);
                    }
                }
            }
        }
    }
}
fn response(status: u16, body: &[u8], close: bool, out: &mut Buffer) {
    out.extend(match status {
        200 => b"HTTP/1.1 200 OK\r\n",
        201 => b"HTTP/1.1 201 Created\r\n",
        400 => b"HTTP/1.1 400 Bad Request\r\n",
        401 => b"HTTP/1.1 401 Unauthorized\r\n",
        404 => b"HTTP/1.1 404 Not Found\r\n",
        503 => b"HTTP/1.1 503 Service Unavailable\r\n",
        _ => b"HTTP/1.1 500 Internal Server Error\r\n",
    });
    out.extend(b"Content-Type: application/json\r\nContent-Length: ");
    out.extend(itoa::Buffer::new().format(body.len()).as_bytes());
    out.extend(if close {
        b"\r\nConnection: close\r\n\r\n"
    } else {
        b"\r\n\r\n"
    });
    out.extend(body);
}
fn listen_addresses(address: &str) -> io::Result<Vec<String>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidInput, "invalid TCP port range");
    let (host, ports) = address.rsplit_once(':').ok_or_else(invalid)?;
    let Some((first, last)) = ports.split_once('-') else {
        return Ok(vec![address.to_owned()]);
    };
    let first = first.parse::<u16>().map_err(|_| invalid())?;
    let last = last.parse::<u16>().map_err(|_| invalid())?;
    if first == 0 || first > last {
        return Err(invalid());
    }
    Ok((first..=last)
        .map(|port| format!("{host}:{port}"))
        .collect())
}

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use std::{
        ffi::CString,
        io::Read,
        os::unix::{fs::OpenOptionsExt, net::UnixStream},
    };

    #[test]
    fn fixed_slot_waits_for_shutdown_even_when_receive_finishes_first() {
        let fifo =
            std::env::temp_dir().join(format!("scooter-shutdown-{}.fifo", std::process::id()));
        let name = CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: live NUL-terminated path; the unique test owns this FIFO.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let (old, mut old_peer) = UnixStream::pair().unwrap();
        let (new, mut new_peer) = UnixStream::pair().unwrap();
        let mut w = Worker::new(Vec::new(), 4096, Tuning::default()).unwrap();
        w.ring
            .submitter()
            .register_iowq_max_workers(&mut [1, 1])
            .unwrap();
        w.ring
            .submitter()
            .register_files_update(0, &[old.as_raw_fd()])
            .unwrap();
        w.clients.push(Client {
            state: LIVE | RECEIVING | CLOSING,
            ..Client::default()
        });
        w.open = 1;
        // Hold the worker so receive EOF can precede queued shutdown execution.
        // name remains live until the OpenAt completion has been consumed.
        w.queue(
            opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), name.as_ptr())
                .flags(libc::O_RDONLY)
                .build()
                .flags(squeue::Flags::ASYNC)
                .user_data(IGNORE),
        );
        w.ring.submit().unwrap();
        let mut blocked = false;
        for _ in 0..100 {
            for task in std::fs::read_dir("/proc/self/task").unwrap() {
                let p = task.unwrap().path();
                let comm = std::fs::read_to_string(p.join("comm")).unwrap_or_default();
                let wait = std::fs::read_to_string(p.join("wchan")).unwrap_or_default();
                blocked |= comm.starts_with("iou-wrk") && wait.contains("wait_for_partner");
            }
            if blocked {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        w.close(0);
        w.ring.submit().unwrap();
        // on_recv clears RECEIVING on its final CQE before calling close.
        w.clients[0].state &= !RECEIVING;
        w.close(0);
        let retired_early = w.clients[0].state & LIVE == 0;
        w.ring.submit().unwrap();
        if retired_early {
            w.ring
                .submitter()
                .register_files_update(0, &[new.as_raw_fd()])
                .unwrap();
        }
        let writer = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&fifo)
            .unwrap();
        let mut completed = 0;
        while completed < 2 {
            w.ring.submit_and_wait(1).unwrap();
            let cqes: Vec<_> = w
                .ring
                .completion()
                .map(|c| (c.user_data(), c.result()))
                .collect();
            for (tag, result) in cqes {
                assert!(result >= 0, "completion {tag}: {result}");
                if tag & 255 == SHUTDOWN {
                    w.on_shutdown(0, 0, result);
                } else {
                    // SAFETY: OpenAt transferred this ordinary owned descriptor.
                    unsafe {
                        libc::close(result);
                    }
                }
                completed += 1;
            }
        }
        w.ring.submit().unwrap();
        if !retired_early {
            w.ring
                .submitter()
                .register_files_update(0, &[new.as_raw_fd()])
                .unwrap();
        }
        old_peer
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        new_peer
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let old_read = old_peer.read(&mut [0u8; 1]);
        let new_read = new_peer.read(&mut [0u8; 1]);
        drop(writer);
        std::fs::remove_file(fifo).unwrap();
        assert!(blocked, "worker was not held");
        assert!(
            !retired_early,
            "slot retired while shutdown still used its index"
        );
        assert!(matches!(old_read, Ok(0)), "old socket: {old_read:?}");
        assert!(
            matches!(new_read, Err(ref e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut)),
            "new socket: {new_read:?}"
        );
        assert_eq!(w.open, 0);
    }
}

#[cfg(test)]
mod multiport_tests {
    use super::listen_addresses;

    #[test]
    fn expands_ports_and_preserves_single_listener() {
        assert_eq!(
            listen_addresses("127.0.0.1:3001").unwrap(),
            ["127.0.0.1:3001"]
        );
        assert_eq!(
            listen_addresses("0.0.0.0:3100-3109").unwrap(),
            (3100..=3109)
                .map(|port| format!("0.0.0.0:{port}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            listen_addresses("[::1]:65535-65535").unwrap(),
            ["[::1]:65535"]
        );
    }

    #[test]
    fn rejects_invalid_ranges() {
        for address in [
            "127.0.0.1:3109-3100",
            "127.0.0.1:0-1",
            "127.0.0.1:1-65536",
            "127.0.0.1:1-",
            "127.0.0.1:1-2-3",
        ] {
            assert!(listen_addresses(address).is_err(), "{address}");
        }
    }
}

pub fn run(address: &str, app: App, tuning: Tuning) -> io::Result<()> {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let slots = limits.rlim_max.min(TARGET_SLOTS as libc::rlim_t) as usize;
    if slots < 4096 {
        return Err(io::Error::other(
            "descriptor hard limit must be at least 4096",
        ));
    }
    let workers = TARGET_SLOTS.div_ceil(slots).min(MAX_WORKERS);
    if limits.rlim_cur < slots as libc::rlim_t {
        limits.rlim_cur = slots as libc::rlim_t;
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    // Ring teardown after SIGKILL is asynchronous and may briefly retain an
    // accept's listener reference. Retry only address-in-use, well within the
    // challenge's 60-second startup allowance.
    let mut attempt = 0;
    let mut listeners = Vec::new();
    for address in listen_addresses(address)? {
        let listener = loop {
            match TcpListener::bind(&address) {
                Ok(listener) => break listener,
                Err(e) if e.raw_os_error() == Some(libc::EADDRINUSE) && attempt < 200 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(e),
            }
        };
        let fd = listener.as_raw_fd();
        let enabled = 1i32;
        // SAFETY: listener owns this live socket; enabled is a readable i32 for
        // the duration of setsockopt. All listeners outlive the worker threads.
        unsafe {
            if libc::listen(fd, 4096) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                (&enabled as *const i32).cast(),
                std::mem::size_of_val(&enabled) as libc::socklen_t,
            ) != 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        listeners.push(listener);
    }
    let shared = Arc::new(Mutex::new(app));
    let ports = sql::start(shared, workers)?;
    for (id, port) in ports.into_iter().enumerate() {
        let fds = listeners.iter().map(AsRawFd::as_raw_fd).collect();
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(format!("ring-{id}"))
            .stack_size(1024 * 1024)
            .spawn(move || match Worker::new(fds, slots, tuning) {
                Ok(mut w) => {
                    w.accept();
                    match w.ring.submit() {
                        Ok(_) => {
                            tx.send(Ok(())).unwrap();
                            w.run(port);
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                }
            })?;
        rx.recv().map_err(io::Error::other)??;
    }
    eprintln!(
        "listening {address}; io_uring, {workers} rings x {slots} direct descriptors; SQLite WAL/NORMAL"
    );
    loop {
        std::thread::park_timeout(Duration::from_secs(3600));
    }
}

#[cfg(test)]
mod sql_tests {
    use super::*;
    use std::{io::Read, net::Shutdown, os::unix::net::UnixStream};

    #[test]
    fn gated_sql_recycles_receive_and_preserves_half_closed_pipeline() {
        assert_eq!(std::mem::size_of::<Client>(), 64);
        let path = std::env::temp_dir().join(format!("scooter-offload-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let app = Arc::new(Mutex::new(App {
            db: crate::db::test_fixture(path.to_str().unwrap()),
            auth: crate::Auth::new("test-secret"),
            start: Instant::now(),
        }));
        let gate = app.lock().unwrap();
        let port = sql::start(Arc::clone(&app), 1).unwrap().remove(0);
        let (socket, mut peer) = UnixStream::pair().unwrap();
        peer.set_nonblocking(true).unwrap();
        let mut w = Worker::new(Vec::new(), 4096, Tuning::default()).unwrap();
        w.ring
            .submitter()
            .register_files_update(0, &[socket.as_raw_fd()])
            .unwrap();
        drop(socket); // Only the registered fixed slot now owns the server end.
        w.clients.push(Client {
            state: LIVE | RECEIVING,
            ..Client::default()
        });
        w.open = 1;
        w.sql = Some(port);
        w.arm_sql();
        let request = b"GET /health HTTP/1.1\r\nHost: local\r\n\r\nGET /health HTTP/1.1\r\nHost: local\r\n\r\n";
        // SAFETY: test owns the receive mapping and this initialized prefix is
        // within buffer0. Inject the same selected-buffer CQE ownership as recv.
        unsafe {
            ptr::copy_nonoverlapping(request.as_ptr(), w.recv_memory.ptr.as_ptr(), request.len());
        }
        let old_tail = w.tail;
        w.on_recv(0, 0, request.len() as i32, 3); // BUFFER | MORE, buffer id0.
        assert_ne!(w.clients[0].state & SQL_PENDING, 0);
        if w.buf_ring.is_some() {
            assert_eq!(
                w.tail,
                old_tail.wrapping_add(1),
                "receive buffer must recycle before SQL can run"
            );
        } else {
            assert_eq!(
                w.returned,
                [0],
                "receive buffer must recycle before SQL can run"
            );
        }
        assert_ne!(w.clients[0].input.len, 0, "pipeline tail must remain owned");
        assert_eq!(
            w.clients[0].input.cap, 256,
            "small input must avoid4KiB padding"
        );
        assert_eq!(w.input_cap_bytes, 256);
        peer.shutdown(Shutdown::Write).unwrap();
        w.on_recv(0, 0, 0, 0);
        assert_ne!(w.clients[0].state & LIVE, 0, "pending SQL retains the slot");
        w.queue(opcode::Nop::new().build().user_data(IGNORE));
        w.ring.submit_and_wait(1).unwrap();
        assert!(
            w.ring.completion().any(|c| c.user_data() == IGNORE),
            "ring completion work must progress while SQL is gated"
        );
        assert!(w.sql.as_ref().unwrap().reply().is_none());
        drop(gate);
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut bytes = Vec::new();
        let mut eof = false;
        while Instant::now() < deadline && !eof {
            while let Some((fd, generation)) = w.ready.pop_front() {
                if w.clients[fd].generation == generation {
                    w.clients[fd].state &= !READY;
                    w.process(fd, &[]);
                }
            }
            let timeout = types::Timespec::new().nsec(1_000_000);
            let result = w
                .ring
                .submitter()
                .submit_with_args(1, &types::SubmitArgs::new().timespec(&timeout));
            if let Err(e) = result {
                assert!(
                    matches!(e.raw_os_error(), Some(libc::ETIME | libc::EINTR)),
                    "{e}"
                );
            }
            let cqes: Vec<_> = w
                .ring
                .completion()
                .map(|c| (c.user_data(), c.result()))
                .collect();
            for (tag, result) in cqes {
                match tag & 255 {
                    SQL_READY => w.sql_ready(result),
                    SEND => w.on_send(((tag >> 8) & 0xffffff) as usize, (tag >> 32) as u32, result),
                    SHUTDOWN => w.on_shutdown(0, 0, result),
                    _ => {}
                }
            }
            w.flush(true);
            let mut scratch = [0u8; 4096];
            loop {
                match peer.read(&mut scratch) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(n) => bytes.extend_from_slice(&scratch[..n]),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => panic!("peer: {e}"),
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            eof,
            "server must close after draining the half-closed pipeline; state={} input={} output={} flight={} bytes={:?}",
            w.clients[0].state,
            w.clients[0].input.len,
            w.clients[0].output.len,
            w.clients[0].flight.len,
            String::from_utf8_lossy(&bytes)
        );
        let response = String::from_utf8(bytes).unwrap();
        assert_eq!(response.matches("HTTP/1.1 200 OK").count(), 2, "{response}");
        assert_eq!(
            response.matches("\"status\":\"ok\"").count(),
            2,
            "{response}"
        );
        assert_eq!(w.open, 0);
        assert_eq!(w.input_cap_bytes, 0);
        drop(w);
        drop(app);
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod sql_wait_tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    #[test]
    fn stale_waiters_cannot_strand_a_live_waiter_after_capacity_wake() {
        let path = std::env::temp_dir().join(format!("scooter-waiters-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let app = Arc::new(Mutex::new(App {
            db: crate::db::test_fixture(path.to_str().unwrap()),
            auth: crate::Auth::new("test-secret"),
            start: Instant::now(),
        }));
        // Two ports model one ring freeing global capacity while the other has
        // only waiters and therefore no SQL reply of its own to wake it again.
        let mut ports = sql::start(app, 2).unwrap();
        let port = ports.pop().unwrap();
        let mut w = Worker::new(Vec::new(), 4096, Tuning::default()).unwrap();
        w.sql = Some(port);
        w.clients.push(Client {
            state: LIVE | SQL_WAIT,
            generation: 1,
            ..Client::default()
        });
        for _ in 0..130 {
            w.sql_waiters.push_back((0, 0));
        }
        w.sql_waiters.push_back((0, 1));
        w.sql_ready(i32::from(libc::POLLIN));
        assert_eq!(w.sql_waiters.len(), 3);
        // The first fairness slice contains only stale entries. It must arrange
        // another wake without relying on any future SQL completion.
        w.ring.submit_and_wait(1).unwrap();
        let events: Vec<_> = w
            .ring
            .completion()
            .map(|c| (c.user_data(), c.result()))
            .collect();
        assert!(events.iter().any(|(tag, _)| *tag == SQL_READY));
        for (tag, result) in events {
            if tag == SQL_READY {
                w.sql_ready(result);
            }
        }
        assert!(w.sql_waiters.is_empty());
        assert_eq!(w.clients[0].state & SQL_WAIT, 0);
        assert_eq!(w.ready.front(), Some(&(0, 1)));
        drop(w);
        drop(ports);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn final_receive_retires_already_closing_sql_waiter() {
        let (socket, _peer) = UnixStream::pair().unwrap();
        let mut w = Worker::new(Vec::new(), 4096, Tuning::default()).unwrap();
        w.ring
            .submitter()
            .register_files_update(0, &[socket.as_raw_fd()])
            .unwrap();
        drop(socket);
        w.clients.push(Client {
            state: LIVE | RECEIVING | CLOSING | SHUT | SQL_WAIT,
            ..Client::default()
        });
        w.open = 1;
        w.on_recv(0, 0, 0, 0);
        assert_eq!(
            w.open, 0,
            "no SQL job owns this slot; EOF must finish retirement"
        );
        assert_eq!(w.clients[0].state & LIVE, 0);
        w.ring.submit().unwrap();
    }
}
