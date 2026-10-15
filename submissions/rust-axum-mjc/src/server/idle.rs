//! Keep idle sockets without an HTTP future, task, or Tokio registration.
//! Active connections keep Hyper state until both input and output are complete.

use super::{AcceptErrorAction, accept_any, accept_error_action, socket::Socket};
use crate::web::Response;
use axum::http::Request;
use hyper::{body::Incoming, server::conn::http1, service::Service};
use hyper_util::rt::TokioIo;
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io,
    net::TcpStream,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    pin::Pin,
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::{
    io::unix::AsyncFd,
    net::TcpListener,
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

const IDLE_READY_BATCH: usize = 1024;
const MAX_ACTIVE_CONNECTIONS: usize = 8192;

struct IdleConnection<S> {
    stream: TcpStream,
    service: S,
}

struct IdleSockets<S> {
    connections: HashMap<u64, IdleConnection<S>>,
    readiness: AsyncFd<OwnedFd>,
    next_token: u64,
}

impl<S> IdleSockets<S> {
    fn new() -> io::Result<Self> {
        // SAFETY: epoll_create1 has no pointer arguments and returns a new FD.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful call created this FD; no other owner exists.
        let epoll = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self {
            connections: HashMap::new(),
            readiness: AsyncFd::new(epoll)?,
            next_token: 0,
        })
    }

    fn push(&mut self, connection: IdleConnection<S>, token: Option<u64>) -> io::Result<()> {
        let (token, operation) = match token {
            Some(token) => (token, libc::EPOLL_CTL_MOD),
            None => {
                let token = self.next_token;
                self.next_token = token
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("idle socket token space exhausted"))?;
                (token, libc::EPOLL_CTL_ADD)
            }
        };
        // One-shot delivery disables this entry while the socket is active.
        // MOD rearms the same entry and rechecks current readiness, including
        // bytes that arrived before returning from Hyper or Tokio into std.
        let mut event = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLONESHOT) as u32,
            u64: token,
        };
        // SAFETY: both FDs are owned and live throughout this synchronous call.
        // epoll_ctl copies the initialized event; it does not retain its address.
        let result = unsafe {
            libc::epoll_ctl(
                self.readiness.get_ref().as_raw_fd(),
                operation,
                connection.stream.as_raw_fd(),
                &mut event,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        // Tokens follow their socket through active ownership and are never
        // reused for another socket, even when the OS reuses a closed FD.
        self.connections.insert(token, connection);
        Ok(())
    }

    async fn ready(
        &mut self,
        events: &mut [libc::epoll_event; IDLE_READY_BATCH],
    ) -> io::Result<(usize, u128)> {
        // One Tokio registration watches the idle epoll instance. Waiting here
        // does not create a task or ScheduledIo allocation per idle socket.
        self.readiness
            .async_io_mut(tokio::io::Interest::READABLE, |epoll| {
                let started = Instant::now();
                // SAFETY: the owned epoll FD is live; events is writable for
                // exactly IDLE_READY_BATCH initialized entries. Timeout zero
                // cannot block the runtime. The kernel returns at most that many.
                let count = unsafe {
                    libc::epoll_wait(
                        epoll.as_raw_fd(),
                        events.as_mut_ptr(),
                        IDLE_READY_BATCH as libc::c_int,
                        0,
                    )
                };
                match count {
                    -1 => Err(io::Error::last_os_error()),
                    0 => Err(io::ErrorKind::WouldBlock.into()),
                    _ => Ok((count as usize, started.elapsed().as_micros())),
                }
            })
            .await
    }

    fn take_ready(&mut self, token: u64) -> Option<IdleConnection<S>> {
        // Removing the sole idle owner also makes duplicate/stale events inert.
        // The one-shot kernel entry stays disabled until that owner comes back.
        self.connections.remove(&token)
    }

    fn release_empty_storage(&mut self) {
        if self.connections.is_empty() {
            self.connections = HashMap::new();
        }
    }
}

struct ActiveConnection<S>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible>,
{
    http: Option<http1::Connection<TokioIo<Socket>, S>>,
    token: u64,
}

impl<S> ActiveConnection<S>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible>,
{
    fn new(connection: IdleConnection<S>, token: u64, builder: &http1::Builder) -> Self {
        Self {
            http: Some(builder.serve_connection(
                TokioIo::new(Socket::new(connection.stream)),
                connection.service,
            )),
            token,
        }
    }

    fn shutdown(&mut self) {
        if let Some(http) = &mut self.http {
            Pin::new(http).graceful_shutdown();
        }
    }
}

impl<S> Future for ActiveConnection<S>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible>,
{
    type Output = io::Result<Option<(u64, IdleConnection<S>)>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let http = this.http.as_mut().expect("HTTP connection missing");
        let mut result = http.poll_until_idle(cx);
        if result.is_pending() && http.io_mut().inner_mut().register_if_blocked()? {
            // Register before retrying I/O so a readiness edge between the
            // original WouldBlock and this poll cannot leave HTTP asleep.
            result = http.poll_until_idle(cx);
        }
        let idle = ready!(result);
        let connection = this.http.take().expect("HTTP connection missing");
        Poll::Ready(match idle {
            Ok(true) => {
                let parts = connection.into_parts();
                // Hyper only yields idle when no partial/pipelined input remains.
                debug_assert!(parts.read_buf.is_empty());
                parts.io.into_inner().into_std().map(|stream| {
                    Some((
                        this.token,
                        IdleConnection {
                            stream,
                            service: parts.service,
                        },
                    ))
                })
            }
            Ok(false) => Ok(None),
            Err(error) => Err(io::Error::other(error)),
        })
    }
}

pub(super) async fn serve_many<S, F>(
    listeners: Vec<TcpListener>,
    service: S,
    shutdown: F,
) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    F: Future<Output = ()>,
{
    let mut builder = http1::Builder::new();
    builder
        .header_read_timeout(None)
        .writev(true)
        .half_close(true);
    let mut idle = IdleSockets::new()?;
    let mut events = [libc::epoll_event { events: 0, u64: 0 }; IDLE_READY_BATCH];
    let mut active = JoinSet::new();
    let (stop, _) = watch::channel(false);
    let mut report = tokio::time::interval(Duration::from_secs(1));
    report.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut next_listener = 0;
    let mut accept_after = Instant::now();
    let timings = std::env::var("SQLITE_TIMINGS").is_ok_and(|value| value == "true");
    let mut report_at = Instant::now();
    let (mut accepted_count, mut completed_count, mut ready_count) = (0_u64, 0_u64, 0_u64);
    let (mut scan_us, mut max_scan_us) = (0_u128, 0_u128);
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = async {
                tokio::time::sleep_until(accept_after).await;
                accept_any(&listeners, &mut next_listener).await
            } => match accepted {
                Ok((stream, _)) => {
                    accepted_count += 1;
                    if let Err(error) = stream.set_nodelay(true) {
                        eprintln!("failed to set TCP_NODELAY: {error}");
                    }
                    idle.push(IdleConnection { stream: stream.into_std()?, service: service.clone() }, None)?;
                }
                Err(error) => if accept_error_action(&error) == AcceptErrorAction::Backoff {
                    eprintln!("failed to accept connection: {error}");
                    accept_after = Instant::now() + Duration::from_secs(1);
                },
            },
            // Leave unread sockets in epoll when active work fills the budget.
            // Reserve room for the entire returned batch before polling: no
            // readiness event is consumed without space for its HTTP task.
            // Completed tasks remain counted until join_next releases them.
            readiness = idle.ready(&mut events),
                if active.len() <= MAX_ACTIVE_CONNECTIONS - IDLE_READY_BATCH => {
                let (count, elapsed) = match readiness {
                    Ok(ready) => ready,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                };
                scan_us += elapsed;
                max_scan_us = max_scan_us.max(elapsed);
                for event in &events[..count] {
                    if let Some(connection) = idle.take_ready(event.u64) {
                        ready_count += 1;
                        let mut connection = ActiveConnection::new(connection, event.u64, &builder);
                        let mut shutdown = stop.subscribe();
                        // Each active connection needs its own Tokio
                        // cooperative budget. A FuturesUnordered shares
                        // its parent's budget across every HTTP future.
                        active.spawn(async move {
                            tokio::select! {
                                result = &mut connection => result,
                                _ = shutdown.changed() => {
                                    connection.shutdown();
                                    connection.await
                                }
                            }
                        });
                    }
                }
            },
            _ = report.tick() => {
                if active.is_empty() { idle.release_empty_storage(); }
                if timings && report_at.elapsed() >= Duration::from_secs(1) {
                    eprintln!("HTTP_CONNECTIONS {}", serde_json::json!({
                        "end_unix_ms": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis(),
                        "interval_us": report_at.elapsed().as_micros(),
                        "idle": idle.connections.len(), "active": active.len(),
                        "active_limit": MAX_ACTIVE_CONNECTIONS,
                        "idle_capacity": idle.connections.capacity(),
                        "accepted": accepted_count, "completed": completed_count, "ready": ready_count,
                        "readiness_us": scan_us, "max_readiness_us": max_scan_us,
                    }));
                    report_at = Instant::now();
                    (accepted_count, completed_count, ready_count) = (0, 0, 0);
                    (scan_us, max_scan_us) = (0, 0);
                }
            },
            completed = active.join_next(), if !active.is_empty() => {
                completed_count += 1;
                match completed.expect("active connection missing") {
                    Ok(Ok(Some((token, connection)))) => idle.push(connection, Some(token))?,
                    Ok(Ok(None)) => {},
                    Ok(Err(error)) => eprintln!("connection failed: {error}"),
                    Err(error) => eprintln!("connection task failed: {error}"),
                }
            },
        }
    }
    drop(listeners);
    // Closing the shared epoll drops its disabled active entries. Active tasks
    // own their sockets independently and can finish shutdown without rearming.
    drop(idle);
    let _ = stop.send(true);
    while let Some(result) = active.join_next().await {
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => eprintln!("connection failed during shutdown: {error}"),
            Err(error) => eprintln!("connection task failed during shutdown: {error}"),
        }
    }
    Ok(())
}
