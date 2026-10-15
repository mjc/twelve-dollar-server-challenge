//! Keep idle sockets without an HTTP future, task, or Tokio registration.
//! Active connections keep Hyper state until both input and output are complete.

use super::{AcceptErrorAction, accept_any, accept_error_action};
use crate::web::Response;
use axum::http::Request;
use hyper::{body::Incoming, server::conn::http1, service::Service};
use hyper_util::rt::TokioIo;
use mio::{Events, Interest, Token, unix::SourceFd};
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io,
    net::TcpStream,
    os::fd::AsRawFd,
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
    connections: HashMap<Token, IdleConnection<S>>,
    readiness: AsyncFd<mio::Poll>,
    next_token: usize,
}

impl<S> IdleSockets<S> {
    fn new() -> io::Result<Self> {
        Ok(Self {
            connections: HashMap::new(),
            readiness: AsyncFd::new(mio::Poll::new()?)?,
            next_token: 0,
        })
    }

    fn push(&mut self, connection: IdleConnection<S>) -> io::Result<()> {
        let token = Token(self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or_else(|| io::Error::other("idle socket token space exhausted"))?;
        let fd = connection.stream.as_raw_fd();
        // The map owns the socket until deregistration. Tokens are never reused:
        // a previously delivered event cannot select a replacement for a closed fd.
        self.readiness.get_ref().registry().register(
            &mut SourceFd(&fd),
            token,
            Interest::READABLE,
        )?;
        self.connections.insert(token, connection);
        Ok(())
    }

    async fn ready(&mut self, events: &mut Events) -> io::Result<u128> {
        // One Tokio registration watches the idle epoll instance. Waiting here
        // does not create a task or ScheduledIo allocation per idle socket.
        self.readiness
            .async_io_mut(tokio::io::Interest::READABLE, |poll| {
                let started = Instant::now();
                poll.poll(events, Some(Duration::ZERO))?;
                if events.is_empty() {
                    Err(io::ErrorKind::WouldBlock.into())
                } else {
                    Ok(started.elapsed().as_micros())
                }
            })
            .await
    }

    fn take_ready(&mut self, token: Token) -> io::Result<Option<IdleConnection<S>>> {
        let Some(connection) = self.connections.remove(&token) else {
            return Ok(None);
        };
        let fd = connection.stream.as_raw_fd();
        self.readiness
            .get_ref()
            .registry()
            .deregister(&mut SourceFd(&fd))?;
        Ok(Some(connection))
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
    http: Option<http1::Connection<TokioIo<tokio::net::TcpStream>, S>>,
}

impl<S> ActiveConnection<S>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible>,
{
    fn new(connection: IdleConnection<S>, builder: &http1::Builder) -> io::Result<Self> {
        let stream = tokio::net::TcpStream::from_std(connection.stream)?;
        Ok(Self {
            http: Some(builder.serve_connection(TokioIo::new(stream), connection.service)),
        })
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
    type Output = io::Result<Option<IdleConnection<S>>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let idle = ready!(
            this.http
                .as_mut()
                .expect("HTTP connection missing")
                .poll_until_idle(cx)
        );
        let connection = this.http.take().expect("HTTP connection missing");
        Poll::Ready(match idle {
            Ok(true) => {
                let parts = connection.into_parts();
                // Hyper only yields idle when no partial/pipelined input remains.
                debug_assert!(parts.read_buf.is_empty());
                parts.io.into_inner().into_std().map(|stream| {
                    Some(IdleConnection {
                        stream,
                        service: parts.service,
                    })
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
    builder.header_read_timeout(None).writev(false);
    let mut idle = IdleSockets::new()?;
    let mut events = Events::with_capacity(IDLE_READY_BATCH);
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
                    idle.push(IdleConnection { stream: stream.into_std()?, service: service.clone() })?;
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
                let elapsed = match readiness {
                    Ok(elapsed) => elapsed,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                };
                scan_us += elapsed;
                max_scan_us = max_scan_us.max(elapsed);
                for event in &events {
                    if let Some(connection) = idle.take_ready(event.token())? {
                        ready_count += 1;
                        match ActiveConnection::new(connection, &builder) {
                            Ok(mut connection) => {
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
                            },
                            Err(error) => eprintln!("failed to register active socket: {error}"),
                        }
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
                    Ok(Ok(Some(connection))) => idle.push(connection)?,
                    Ok(Ok(None)) => {},
                    Ok(Err(error)) => eprintln!("connection failed: {error}"),
                    Err(error) => eprintln!("connection task failed: {error}"),
                }
            },
        }
    }
    drop(listeners);
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
