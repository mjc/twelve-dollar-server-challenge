//! Bounded thread-local reuse across active connections, rather than idle ones.
use std::cell::RefCell;

use bytes::BytesMut;
use http::HeaderMap;

const MAX_BUFFERS: usize = 128;
pub(super) const MAX_BUFFER_CAPACITY: usize = 8192;
const MAX_HEADER_CAPACITY: usize = 32;

thread_local! {
    static READS: RefCell<Vec<BytesMut>> = const { RefCell::new(Vec::new()) };
    static WRITES: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
    static HEADERS: RefCell<Vec<HeaderMap>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn take_read() -> BytesMut {
    READS.with(|pool| pool.borrow_mut().pop().unwrap_or_default())
}

pub(super) fn recycle_read(buffer: &mut BytesMut) {
    debug_assert!(buffer.is_empty());
    let buffer = std::mem::take(buffer);
    if buffer.capacity() > 0 && buffer.capacity() <= MAX_BUFFER_CAPACITY {
        let _ = READS.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_BUFFERS {
                pool.push(buffer);
            }
        });
    }
}

pub(super) fn take_write() -> Vec<u8> {
    WRITES.with(|pool| pool.borrow_mut().pop().unwrap_or_default())
}

pub(super) fn recycle_write(buffer: &mut Vec<u8>) {
    debug_assert!(buffer.is_empty());
    let buffer = std::mem::take(buffer);
    if buffer.capacity() > 0 && buffer.capacity() <= MAX_BUFFER_CAPACITY {
        let _ = WRITES.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_BUFFERS {
                pool.push(buffer);
            }
        });
    }
}

pub(super) fn take_headers() -> HeaderMap {
    HEADERS.with(|pool| pool.borrow_mut().pop().unwrap_or_default())
}

pub(super) fn recycle_headers(headers: HeaderMap) {
    debug_assert!(headers.is_empty());
    if headers.capacity() > 0 && headers.capacity() <= MAX_HEADER_CAPACITY {
        let _ = HEADERS.try_with(|pool| {
            let mut pool = pool.borrow_mut();
            if pool.len() < MAX_BUFFERS {
                pool.push(headers);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_storage_and_bounds_retained_capacity() {
        READS.with(|pool| pool.borrow_mut().clear());
        WRITES.with(|pool| pool.borrow_mut().clear());
        HEADERS.with(|pool| pool.borrow_mut().clear());
        let mut read = BytesMut::with_capacity(1024);
        let pointer = read.as_ptr();
        recycle_read(&mut read);
        assert_eq!(read.capacity(), 0);
        let reused_read = take_read();
        assert_eq!(reused_read.as_ptr(), pointer);
        let mut write = Vec::with_capacity(1024);
        let pointer = write.as_ptr();
        recycle_write(&mut write);
        assert_eq!(write.capacity(), 0);
        let reused_write = take_write();
        assert_eq!(reused_write.as_ptr(), pointer);
        recycle_headers(HeaderMap::with_capacity(16));
        assert!(take_headers().capacity() >= 16);
        for _ in 0..MAX_BUFFERS + 1 {
            recycle_read(&mut BytesMut::with_capacity(1024));
            recycle_write(&mut Vec::with_capacity(1024));
            recycle_headers(HeaderMap::with_capacity(16));
        }
        READS.with(|pool| assert_eq!(pool.borrow().len(), MAX_BUFFERS));
        WRITES.with(|pool| assert_eq!(pool.borrow().len(), MAX_BUFFERS));
        HEADERS.with(|pool| assert_eq!(pool.borrow().len(), MAX_BUFFERS));
        READS.with(|pool| pool.borrow_mut().clear());
        WRITES.with(|pool| pool.borrow_mut().clear());
        HEADERS.with(|pool| pool.borrow_mut().clear());
        recycle_read(&mut BytesMut::with_capacity(MAX_BUFFER_CAPACITY * 2));
        recycle_write(&mut Vec::with_capacity(MAX_BUFFER_CAPACITY * 2));
        recycle_headers(HeaderMap::with_capacity(MAX_HEADER_CAPACITY * 2));
        READS.with(|pool| assert!(pool.borrow().is_empty()));
        WRITES.with(|pool| assert!(pool.borrow().is_empty()));
        HEADERS.with(|pool| assert!(pool.borrow().is_empty()));
    }
}
