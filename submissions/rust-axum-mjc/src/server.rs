use crate::web::Response;
use axum::http::Request;
#[cfg(not(target_os = "linux"))]
use futures_util::FutureExt;
#[cfg(not(target_os = "linux"))]
use hyper::server::conn::http1;
use hyper::{body::Incoming, service::Service};
#[cfg(not(target_os = "linux"))]
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
#[cfg(any(test, not(target_os = "linux")))]
use std::time::Duration;
use std::{
    convert::Infallible,
    future::{Future, poll_fn},
    io,
    task::Poll,
};
use tokio::net::TcpListener;

#[cfg(target_os = "linux")]
mod idle;

pub async fn serve<S, F>(listener: TcpListener, service: S, shutdown: F) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    F: Future<Output = ()>,
{
    serve_many(vec![listener], service, shutdown).await
}

/// Serves on all listeners with one shared service and graceful shutdown.
pub async fn serve_many<S, F>(
    listeners: Vec<TcpListener>,
    service: S,
    shutdown: F,
) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    F: Future<Output = ()>,
{
    if listeners.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one TCP listener is required",
        ));
    }

    #[cfg(target_os = "linux")]
    return idle::serve_many(listeners, service, shutdown).await;

    #[cfg(not(target_os = "linux"))]
    serve_many_registered(listeners, service, shutdown).await
}

#[cfg(not(target_os = "linux"))]
async fn serve_many_registered<S, F>(
    listeners: Vec<TcpListener>,
    service: S,
    shutdown: F,
) -> io::Result<()>
where
    S: Service<Request<Incoming>, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    F: Future<Output = ()>,
{
    let mut http = http1::Builder::new();
    // Match Axum's timer-free HTTP/1 configuration and avoid vectored writes.
    http.header_read_timeout(None).writev(false);
    let graceful = GracefulShutdown::new();
    tokio::pin!(shutdown);
    let mut next_listener = 0;
    loop {
        let (stream, _) = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            accepted = accept_any(&listeners, &mut next_listener) => match accepted {
                Ok(connection) => connection,
                Err(error) => {
                    if accept_error_action(&error) == AcceptErrorAction::Backoff {
                        eprintln!("failed to accept connection: {error}");
                        // Resource exhaustion may clear once connections close.
                        // Let shutdown interrupt the delay before retrying.
                        if wait_for_accept_backoff_or_shutdown(&mut shutdown).await {
                            break;
                        }
                    }
                    continue;
                }
            },
        };
        if let Err(error) = stream.set_nodelay(true) {
            eprintln!("failed to set TCP_NODELAY on incoming connection: {error}");
        }
        let service = service.clone();
        let connection = graceful.watch(http.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(connection.map(|result| {
            if let Err(error) = result {
                eprintln!("connection failed: {error}");
            }
        }));
    }
    drop(listeners);
    graceful.shutdown().await;
    Ok(())
}

async fn accept_any(
    listeners: &[TcpListener],
    next_listener: &mut usize,
) -> io::Result<(tokio::net::TcpStream, std::net::SocketAddr)> {
    poll_fn(|cx| {
        let count = listeners.len();
        let start = *next_listener;
        for offset in 0..count {
            let index = (start + offset) % count;
            if let Poll::Ready(accepted) = listeners[index].poll_accept(cx) {
                *next_listener = (index + 1) % count;
                return Poll::Ready(accepted);
            }
        }
        Poll::Pending
    })
    .await
}

#[derive(Debug, PartialEq, Eq)]
enum AcceptErrorAction {
    RetryImmediately,
    Backoff,
}

fn accept_error_action(error: &io::Error) -> AcceptErrorAction {
    match error.kind() {
        io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::NetworkDown
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::Unsupported => return AcceptErrorAction::RetryImmediately,
        _ => {}
    }

    #[cfg(target_os = "linux")]
    if matches!(
        error.raw_os_error(),
        Some(libc::ENETDOWN)
            | Some(libc::EPROTO)
            | Some(libc::ENOPROTOOPT)
            | Some(libc::EHOSTDOWN)
            | Some(libc::ENONET)
            | Some(libc::EHOSTUNREACH)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::ENETUNREACH)
    ) {
        return AcceptErrorAction::RetryImmediately;
    }

    AcceptErrorAction::Backoff
}

#[cfg(any(test, not(target_os = "linux")))]
async fn wait_for_accept_backoff_or_shutdown<F>(shutdown: &mut F) -> bool
where
    F: Future<Output = ()> + Unpin,
{
    tokio::select! {
        biased;
        _ = &mut *shutdown => true,
        _ = tokio::time::sleep(Duration::from_secs(1)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptErrorAction, accept_error_action, serve_many, wait_for_accept_backoff_or_shutdown,
    };
    use axum::http::Request;
    use http_body_util::{BodyExt, Full};
    use std::{io, time::Duration};
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };

    async fn read_response(reader: &mut BufReader<TcpStream>) -> (String, Vec<u8>) {
        let mut headers = String::new();
        let mut line = String::new();
        loop {
            line.clear();
            reader.read_line(&mut line).await.unwrap();
            headers.push_str(&line);
            if line == "\r\n" {
                break;
            }
            assert!(
                !line.is_empty(),
                "connection closed before response headers"
            );
        }
        assert!(headers.starts_with("HTTP/1.1 200 OK"));
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).await.unwrap();
        (headers, body)
    }

    #[test]
    fn connection_accept_errors_retry_without_backoff() {
        for kind in [
            io::ErrorKind::Interrupted,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::ConnectionAborted,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::NetworkDown,
            io::ErrorKind::HostUnreachable,
            io::ErrorKind::NetworkUnreachable,
            io::ErrorKind::Unsupported,
        ] {
            assert_eq!(
                accept_error_action(&io::Error::from(kind)),
                AcceptErrorAction::RetryImmediately
            );
        }
    }

    #[test]
    fn resource_and_unexpected_accept_errors_back_off() {
        for error in [
            io::Error::from(io::ErrorKind::Other),
            io::Error::from(io::ErrorKind::OutOfMemory),
        ] {
            assert_eq!(accept_error_action(&error), AcceptErrorAction::Backoff);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_pending_network_accept_errors_retry_but_resources_back_off() {
        for errno in [
            libc::ENETDOWN,
            libc::EPROTO,
            libc::ENOPROTOOPT,
            libc::EHOSTDOWN,
            libc::ENONET,
            libc::EHOSTUNREACH,
            libc::EOPNOTSUPP,
            libc::ENETUNREACH,
        ] {
            assert_eq!(
                accept_error_action(&io::Error::from_raw_os_error(errno)),
                AcceptErrorAction::RetryImmediately,
                "errno {errno} should retry without backoff"
            );
        }
        for errno in [libc::ENOBUFS, libc::ENOMEM, libc::EMFILE, libc::ENFILE] {
            assert_eq!(
                accept_error_action(&io::Error::from_raw_os_error(errno)),
                AcceptErrorAction::Backoff,
                "errno {errno} should retain backoff"
            );
        }
    }

    #[tokio::test]
    async fn shutdown_interrupts_accept_error_backoff() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = shutdown_tx.send(());
        });
        let mut shutdown = Box::pin(async move {
            let _ = shutdown_rx.await;
        });
        assert!(wait_for_accept_backoff_or_shutdown(&mut shutdown).await);
    }

    #[tokio::test]
    async fn serve_many_rejects_empty_listener_lists() {
        let service = hyper::service::service_fn(|_: Request<hyper::body::Incoming>| async {
            Ok::<_, std::convert::Infallible>(crate::web::Response::new(Full::new(
                bytes::Bytes::from(Vec::new()),
            )))
        });

        let error = serve_many(Vec::new(), service, async {})
            .await
            .expect_err("empty listener lists are invalid");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn tokio_transport_handles_large_pipelined_bodies_and_graceful_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let service =
            hyper::service::service_fn(|request: Request<hyper::body::Incoming>| async move {
                let body = request
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .to_vec();
                Ok::<_, std::convert::Infallible>(crate::web::Response::new(Full::new(
                    bytes::Bytes::from(body),
                )))
            });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            super::serve(listener, service, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        let large_body = vec![b'a'; 9_000];
        let mut requests =
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 9000\r\n\r\n".to_vec();
        requests.extend_from_slice(&large_body);
        requests.extend_from_slice(
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\n\r\nsecond",
        );
        requests.extend_from_slice(b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nthird");
        client.write_all(&requests).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let first_end = response.find("\r\n\r\n").unwrap() + 4;
        let first_headers = &response[..first_end];
        assert!(first_headers.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(
            first_headers
                .to_ascii_lowercase()
                .contains("content-length: 9000\r\n")
        );
        assert!(
            response[first_end..].starts_with(&format!("{}HTTP/1.1 200 OK\r\n", "a".repeat(9_000)))
        );
        let second_start = first_end + large_body.len();
        let second_end = response[second_start..].find("\r\n\r\n").unwrap() + second_start + 4;
        assert!(
            response[second_start..second_end]
                .to_ascii_lowercase()
                .contains("content-length: 6\r\n")
        );
        assert_eq!(&response[second_end..second_end + 6], "second");
        let third_start = second_end + 6;
        let third_end = response[third_start..].find("\r\n\r\n").unwrap() + third_start + 4;
        assert!(
            response[third_start..third_end]
                .to_ascii_lowercase()
                .contains("content-length: 5\r\n")
        );
        assert_eq!(&response[third_end..], "third");

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_waits_for_an_in_flight_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let service = hyper::service::service_fn(move |request: Request<hyper::body::Incoming>| {
            let started_tx = started_tx.clone();
            let mut release_rx = release_rx.clone();
            async move {
                request.into_body().collect().await.unwrap();
                let _ = started_tx.send(());
                while !*release_rx.borrow() {
                    if release_rx.changed().await.is_err() {
                        break;
                    }
                }
                Ok::<_, std::convert::Infallible>(crate::web::Response::new(Full::new(
                    bytes::Bytes::from(b"finished".to_vec()),
                )))
            }
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let mut server = tokio::spawn(async move {
            super::serve(listener, service, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"POST /slow HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        shutdown_tx.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut server)
                .await
                .is_err()
        );

        release_tx.send(true).unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .ends_with("\r\n\r\nfinished")
        );
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn multiple_listeners_share_service_keep_alive_and_shutdown() {
        let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let second_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let service = hyper::service::service_fn(|_: Request<hyper::body::Incoming>| async {
            Ok::<_, std::convert::Infallible>(crate::web::Response::new(Full::new(
                bytes::Bytes::from(b"ok".to_vec()),
            )))
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            serve_many(vec![first_listener, second_listener], service, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        let first = TcpStream::connect(first_address).await.unwrap();
        let second = TcpStream::connect(second_address).await.unwrap();
        let mut first = BufReader::new(first);
        let mut second = BufReader::new(second);
        for (client, path) in [(&mut first, "/first"), (&mut second, "/second")] {
            for _ in 0..2 {
                client
                    .get_mut()
                    .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
                    .await
                    .unwrap();
                let (headers, body) = read_response(client).await;
                assert!(!headers.to_ascii_lowercase().contains("connection: close"));
                assert_eq!(body.as_slice(), b"ok");
            }
        }

        drop((first, second));
        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
        assert!(TcpStream::connect(first_address).await.is_err());
        assert!(TcpStream::connect(second_address).await.is_err());
    }
}
