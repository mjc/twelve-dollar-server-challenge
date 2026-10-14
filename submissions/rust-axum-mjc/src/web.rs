use crate::{
    auth::Auth,
    db::{Db, DbError},
};
use axum::{
    body::{Body, Bytes, HttpBody},
    extract::{FromRequest, State},
    http::{
        HeaderMap, Method, Request, StatusCode,
        header::{ALLOW, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue},
    },
    response::{IntoResponse, Response as AxumResponse},
};
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming as HyperIncoming, service::Service as HyperService};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, convert::Infallible, sync::Arc, time::Instant};
use tower::{Service, service_fn};

pub type Response = AxumResponse<Full<Bytes>>;

struct AppState {
    db: Db,
    auth: Auth,
    started: Instant,
}

pub fn router<B>(
    db: Db,
    auth: Auth,
) -> impl Clone
+ Service<Request<B>, Response = Response, Error = Infallible, Future: Send + 'static>
+ Send
+ 'static
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError> + 'static,
{
    let state = app_state(db, auth);
    service_fn(move |request: Request<B>| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(dispatch(state, request).await) }
    })
}

/// The production Hyper service. Unlike `router`, this avoids the Tower
/// adapter used by the test and benchmark service paths.
pub fn hyper_router(
    db: Db,
    auth: Auth,
) -> impl Clone
+ HyperService<
    Request<HyperIncoming>,
    Response = Response,
    Error = Infallible,
    Future: Send + 'static,
> + Send
+ 'static {
    let state = app_state(db, auth);
    hyper::service::service_fn(move |request: Request<HyperIncoming>| {
        let state = Arc::clone(&state);
        async move { Ok::<_, Infallible>(dispatch(state, request).await) }
    })
}

fn app_state(db: Db, auth: Auth) -> Arc<AppState> {
    Arc::new(AppState {
        db,
        auth,
        started: Instant::now(),
    })
}

enum Route<'a> {
    Health,
    Feed,
    Create,
    Post(&'a str),
    Like(&'a str),
    Missing,
}

fn route(path: &str) -> Route<'_> {
    match path {
        "/health" => Route::Health,
        "/feed" => Route::Feed,
        "/posts" => Route::Create,
        _ => {
            let Some(rest) = path.strip_prefix("/posts/") else {
                return Route::Missing;
            };
            if let Some(id) = rest.strip_suffix("/like")
                && !id.is_empty()
                && !id.contains('/')
            {
                return Route::Like(id);
            }
            if !rest.is_empty() && !rest.contains('/') {
                return Route::Post(rest);
            }
            Route::Missing
        }
    }
}

async fn dispatch<B>(state: Arc<AppState>, request: Request<B>) -> Response
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError> + 'static,
{
    let (mut parts, body) = request.into_parts();
    let method = parts.method.clone();
    let matched = route(parts.uri.path());
    let mut headers = parts.headers;
    let response = match matched {
        Route::Health | Route::Feed | Route::Post(_)
            if method != Method::GET && method != Method::HEAD =>
        {
            method_not_allowed(&mut headers, "GET,HEAD")
        }
        Route::Create | Route::Like(_) if method != Method::POST => {
            method_not_allowed(&mut headers, "POST")
        }
        Route::Health => health(State(state), &mut headers).await,
        Route::Feed => feed(State(state), &mut headers).await,
        Route::Post(raw_id) => match crate::post_id::parse_post_id(raw_id) {
            Some(id) => get_post(State(state), id, &mut headers).await,
            None => error(&mut headers, StatusCode::BAD_REQUEST, "invalid post id"),
        },
        Route::Like(raw_id) => like_post(State(state), raw_id, &mut headers).await,
        Route::Create => {
            let mut request = Request::new(Body::new(body));
            *request.extensions_mut() = std::mem::take(&mut parts.extensions);
            match Bytes::from_request(request, &state).await {
                Ok(bytes) => create_post(State(state), &mut headers, bytes).await,
                Err(rejection) => {
                    let (parts, body) = rejection.into_response().into_parts();
                    let body = body
                        .collect()
                        .await
                        .expect("Axum rejection bodies are infallible")
                        .to_bytes()
                        .to_vec();
                    headers.clear();
                    headers.extend(parts.headers);
                    response(parts.status, body, &mut headers)
                }
            }
        }
        Route::Missing => error(&mut headers, StatusCode::NOT_FOUND, "not found"),
    };

    let mut response = response;
    if method == Method::HEAD
        && !response.headers().contains_key(CONTENT_LENGTH)
        && let Some(size) = response.body().size_hint().exact()
    {
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from(size));
    }
    if method == Method::HEAD {
        *response.body_mut() = Full::new(Bytes::new());
    }
    response
}

fn method_not_allowed(headers: &mut HeaderMap, allow: &'static str) -> Response {
    headers.clear();
    headers.insert(ALLOW, allow.parse().expect("static Allow value"));
    response(StatusCode::METHOD_NOT_ALLOWED, Vec::new(), headers)
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

fn error(headers: &mut HeaderMap, status: StatusCode, message: &str) -> Response {
    json_response(headers, status, ErrorBody { error: message })
}

fn encoded_json(headers: &mut HeaderMap, status: StatusCode, body: impl Into<Bytes>) -> Response {
    headers.clear();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response(status, body, headers)
}

fn json_response<T: Serialize>(headers: &mut HeaderMap, status: StatusCode, body: T) -> Response {
    let Ok(body) = serde_json::to_vec(&body) else {
        return plain_text_response(
            headers,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to serialize JSON body",
        );
    };
    encoded_json(headers, status, body)
}

fn plain_text_response(
    headers: &mut HeaderMap,
    status: StatusCode,
    body: &'static str,
) -> Response {
    headers.clear();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response(status, body.as_bytes().to_vec(), headers)
}

fn response(status: StatusCode, body: impl Into<Bytes>, headers: &mut HeaderMap) -> Response {
    let mut response = Response::new(Full::new(body.into()));
    *response.status_mut() = status;
    *response.headers_mut() = std::mem::take(headers);
    response
}

fn internal_error(headers: &mut HeaderMap, error_value: DbError) -> Response {
    if matches!(&error_value, DbError::WorkerBusy) {
        return error(headers, StatusCode::SERVICE_UNAVAILABLE, "database busy");
    }
    eprintln!("database request failed: {error_value}");
    error(
        headers,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal server error",
    )
}

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
    db: &'static str,
    uptime_s: u64,
}

#[derive(Serialize)]
struct DegradedHealth {
    status: &'static str,
    db: &'static str,
    error: String,
}

async fn health(State(state): State<Arc<AppState>>, headers: &mut HeaderMap) -> Response {
    match state.db.health().await {
        Ok(()) => json_response(
            headers,
            StatusCode::OK,
            HealthBody {
                status: "ok",
                db: "ok",
                uptime_s: state.started.elapsed().as_secs(),
            },
        ),
        Err(error_value) => json_response(
            headers,
            StatusCode::SERVICE_UNAVAILABLE,
            DegradedHealth {
                status: "degraded",
                db: "unreachable",
                error: error_value.to_string(),
            },
        ),
    }
}

async fn feed(State(state): State<Arc<AppState>>, headers: &mut HeaderMap) -> Response {
    match state.db.feed().await {
        Ok(body) => encoded_json(headers, StatusCode::OK, body),
        Err(error_value) => internal_error(headers, error_value),
    }
}

async fn get_post(
    State(state): State<Arc<AppState>>,
    id: i64,
    headers: &mut HeaderMap,
) -> Response {
    match state.db.post(id).await {
        Ok(Some(body)) => encoded_json(headers, StatusCode::OK, body),
        Ok(None) => error(headers, StatusCode::NOT_FOUND, "post not found"),
        Err(error_value) => internal_error(headers, error_value),
    }
}

#[derive(Deserialize)]
struct Incoming<'a> {
    // A direct Cow field lets serde borrow unescaped JSON strings. Missing body
    // defaults to an empty string, which follows the same required-body check.
    #[serde(default, borrow)]
    body: Cow<'a, str>,
}

fn post_body(bytes: &Bytes) -> Result<Bytes, &'static str> {
    let object_start = bytes.iter().position(|byte| !byte.is_ascii_whitespace());
    if !object_start.is_some_and(|index| bytes[index] == b'{') {
        let _: &RawValue = serde_json::from_slice(bytes).map_err(|_| "malformed JSON body")?;
        return Err("body is required");
    }
    // Retain borrowed strings and parse accepted requests once. Rejected
    // type errors get a full-document validation pass to preserve error order.
    let incoming: Incoming<'_> = match serde_json::from_slice(bytes) {
        Ok(incoming) => incoming,
        Err(error) if error.is_data() => {
            // Deserialization can stop at a type error before reaching invalid
            // trailing JSON. Validate those uncommon rejected requests fully
            // to preserve malformed-JSON precedence.
            let _: &RawValue = serde_json::from_slice(bytes).map_err(|_| "malformed JSON body")?;
            return Err("body is required");
        }
        Err(_) => return Err("malformed JSON body"),
    };
    let trimmed = incoming.body.trim();
    if trimmed.is_empty() {
        return Err("body is required");
    }
    if trimmed.chars().count() > 500 {
        return Err("body must be at most 500 characters");
    }
    let start = incoming.body.len() - incoming.body.trim_start().len();
    let end = start + trimmed.len();
    Ok(match incoming.body {
        Cow::Borrowed(text) => bytes.slice_ref(text.trim().as_bytes()),
        // Escapes require decoding; keep that allocation and slice its padding.
        Cow::Owned(text) => Bytes::from(text).slice(start..end),
    })
}

async fn create_post(
    State(state): State<Arc<AppState>>,
    headers: &mut HeaderMap,
    bytes: Bytes,
) -> Response {
    let user = match state.auth.authenticate(headers) {
        Ok(user) => user,
        Err(message) => return error(headers, StatusCode::UNAUTHORIZED, message),
    };
    let body = match post_body(&bytes) {
        Ok(body) => body,
        Err(message) => return error(headers, StatusCode::BAD_REQUEST, message),
    };
    match state
        .db
        .create_post(user.user_id, body, user.username)
        .await
    {
        Ok(body) => encoded_json(headers, StatusCode::CREATED, body),
        Err(error_value) => internal_error(headers, error_value),
    }
}

#[derive(Serialize)]
struct LikeBody {
    liked: bool,
    already_liked: bool,
    post_id: i64,
}

async fn like_post(
    State(state): State<Arc<AppState>>,
    raw_id: &str,
    headers: &mut HeaderMap,
) -> Response {
    let user_id = match state.auth.authenticate_id(headers) {
        Ok(user_id) => user_id,
        Err(message) => return error(headers, StatusCode::UNAUTHORIZED, message),
    };
    let Some(post_id) = crate::post_id::parse_post_id(raw_id) else {
        return error(headers, StatusCode::BAD_REQUEST, "invalid post id");
    };
    match state.db.like(user_id, post_id).await {
        Ok(Some(already_liked)) => json_response(
            headers,
            if already_liked {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            LikeBody {
                liked: true,
                already_liked,
                post_id,
            },
        ),
        Ok(None) => error(headers, StatusCode::NOT_FOUND, "post not found"),
        Err(error_value) => internal_error(headers, error_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, header::CONTENT_TYPE},
    };
    use http_body_util::BodyExt;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::{Value, json};
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpStream,
        path::PathBuf,
        time::Duration,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tempfile::TempDir;
    use tower::{ServiceExt, util::BoxCloneService};

    struct Fixture {
        app: BoxCloneService<axum::extract::Request, Response, Infallible>,
        token: String,
        _directory: TempDir,
    }

    #[test]
    fn plain_body_borrows_request_bytes_and_escaped_body_is_decoded_once() {
        let request = Bytes::from_static(br#"{"body":"  hello  "}"#);
        let body = post_body(&request).unwrap();
        assert_eq!(body, "hello");
        assert_eq!(body.as_ptr(), request[11..].as_ptr());
        let request = Bytes::from_static(br#"{"body":" \u00e9\n\"quoted\" ","ignored":[true,1]}"#);
        assert_eq!(post_body(&request).unwrap(), "é\n\"quoted\"");
        assert_eq!(
            post_body(&Bytes::from_static(br#"{"body":1}garbage"#)),
            Err("malformed JSON body")
        );
        assert_eq!(
            post_body(&Bytes::from_static(br#"{"body":1,"ignored": }"#)),
            Err("malformed JSON body")
        );
        assert_eq!(
            post_body(&Bytes::from_static(br#"{"body":1}"#)),
            Err("body is required")
        );
        assert_eq!(
            post_body(&Bytes::from_static(br#"["hello"]"#)),
            Err("body is required")
        );
        assert_eq!(
            post_body(&Bytes::from_static(br#"{"body":"\uD800"}"#)),
            Err("malformed JSON body")
        );
    }

    #[test]
    fn successful_json_response_reuses_cleared_request_header_storage() {
        let mut headers = HeaderMap::with_capacity(16);
        for index in 0..8 {
            headers.insert(
                format!("x-request-{index}")
                    .parse::<axum::http::HeaderName>()
                    .unwrap(),
                HeaderValue::from_static("private"),
            );
        }
        let header_capacity = headers.capacity();
        let mut body = Vec::with_capacity(64);
        body.extend_from_slice(b"{}");
        let pointer = body.as_ptr();
        let response = encoded_json(&mut headers, StatusCode::OK, body);

        assert_eq!(response.headers().capacity(), header_capacity);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
        assert!(!response.headers().contains_key("x-request-0"));
        let body = response.into_body().into_inner().unwrap();
        assert_eq!(body.as_ptr(), pointer);
        assert_eq!(body.as_ref(), b"{}");
    }

    #[tokio::test]
    async fn response_headers_do_not_reflect_request_headers() {
        let fixture = fixture();
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/posts/1")
                    .header("authorization", "Bearer secret")
                    .header("x-private-request", "private")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
        assert!(!response.headers().contains_key("authorization"));
        assert!(!response.headers().contains_key("x-private-request"));
    }

    fn fixture() -> Fixture {
        let scratch = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
        std::fs::create_dir_all(&scratch).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("web-")
            .tempdir_in(scratch)
            .unwrap();
        let db = seeded_db(&directory, true);
        let token = encode(&Header::new(Algorithm::HS256), &json!({
            "sub":"1", "username":"token-name", "exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        }), &EncodingKey::from_secret(b"test-secret")).unwrap();
        Fixture {
            app: BoxCloneService::new(router(db, Auth::new(b"test-secret"))),
            token,
            _directory: directory,
        }
    }

    fn seeded_db(directory: &TempDir, read_batching: bool) -> Db {
        let path = directory.path().join("feed.db");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../../schema.sql"))
            .unwrap();
        connection.execute_batch("INSERT INTO users (id, username) VALUES (1, 'stored-name'), (2, 'other-user'); INSERT INTO posts (id,user_id,body,created_at) VALUES (1,1,'seed post','2000-01-01T00:00:00.000Z');").unwrap();
        drop(connection);
        Db::open_with_read_batching(&path, read_batching).unwrap()
    }

    fn read_http_response(
        stream: &mut BufReader<TcpStream>,
        body_allowed: bool,
    ) -> (u16, std::collections::HashMap<String, String>, Vec<u8>) {
        let mut line = String::new();
        stream.read_line(&mut line).unwrap();
        let status = line.split_whitespace().nth(1).unwrap().parse().unwrap();
        let mut headers = std::collections::HashMap::new();
        loop {
            line.clear();
            stream.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            let (name, value) = line.split_once(':').unwrap();
            headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        let mut body = vec![
            0;
            if body_allowed {
                headers["content-length"].parse().unwrap()
            } else {
                0
            }
        ];
        stream.read_exact(&mut body).unwrap();
        (status, headers, body)
    }

    async fn request(
        fixture: &Fixture,
        method: &str,
        path: &str,
        authenticated: bool,
        body: &str,
    ) -> (StatusCode, String) {
        let mut builder = Request::builder().method(method).uri(path);
        if authenticated {
            builder = builder.header("Authorization", format!("Bearer {}", fixture.token));
        }
        let response = fixture
            .app
            .clone()
            .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn authentication_precedes_id_and_json_validation() {
        let fixture = fixture();
        assert_eq!(
            request(&fixture, "POST", "/posts/abc/like", false, "").await,
            (
                StatusCode::UNAUTHORIZED,
                r#"{"error":"missing bearer token"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts/%FF/like", false, "").await,
            (
                StatusCode::UNAUTHORIZED,
                r#"{"error":"missing bearer token"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts", false, "{bad json").await,
            (
                StatusCode::UNAUTHORIZED,
                r#"{"error":"missing bearer token"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts/abc/like", true, "").await,
            (
                StatusCode::BAD_REQUEST,
                r#"{"error":"invalid post id"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts/%FF/like", true, "").await,
            (
                StatusCode::BAD_REQUEST,
                r#"{"error":"invalid post id"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts", true, "{bad json").await,
            (
                StatusCode::BAD_REQUEST,
                r#"{"error":"malformed JSON body"}"#.to_owned()
            )
        );
    }

    #[tokio::test]
    async fn unicode_is_trimmed_and_length_counts_characters() {
        let fixture = fixture();
        for value in [
            json!({}),
            json!({"body":123}),
            json!({"body":"\u{2003}\n\t"}),
            json!(null),
        ] {
            assert_eq!(
                request(&fixture, "POST", "/posts", true, &value.to_string()).await,
                (
                    StatusCode::BAD_REQUEST,
                    r#"{"error":"body is required"}"#.to_owned()
                )
            );
        }
        let body = "é".repeat(500);
        let (status, response) = request(
            &fixture,
            "POST",
            "/posts",
            true,
            &json!({"body":format!("\u{2003}{body}\u{2003}")}).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let post: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(post["post"]["body"], body);
        assert_eq!(post["post"]["author"], "token-name");
        assert_eq!(
            request(
                &fixture,
                "POST",
                "/posts",
                true,
                &json!({"body":"é".repeat(501)}).to_string()
            )
            .await,
            (
                StatusCode::BAD_REQUEST,
                r#"{"error":"body must be at most 500 characters"}"#.to_owned()
            )
        );
    }

    #[tokio::test]
    async fn likes_are_idempotent_and_visible_on_the_next_read() {
        let fixture = fixture();
        let (status, response) =
            request(&fixture, "POST", "/posts", true, r#"{"body":"  hello  "}"#).await;
        assert_eq!(status, StatusCode::CREATED);
        let post: Value = serde_json::from_str(&response).unwrap();
        let id = post["post"]["id"].as_i64().unwrap();
        let created = post["post"]["created_at"].as_str().unwrap();
        assert_eq!(
            response,
            format!(
                r#"{{"post":{{"id":{id},"body":"hello","created_at":"{created}","author":"token-name","like_count":0}}}}"#
            )
        );
        assert_eq!(
            request(&fixture, "POST", &format!("/posts/{id}/like"), true, "").await,
            (
                StatusCode::CREATED,
                format!(r#"{{"liked":true,"already_liked":false,"post_id":{id}}}"#)
            )
        );
        assert_eq!(
            request(&fixture, "POST", &format!("/posts/{id}/like"), true, "").await,
            (
                StatusCode::OK,
                format!(r#"{{"liked":true,"already_liked":true,"post_id":{id}}}"#)
            )
        );
        let (status, read) = request(&fixture, "GET", &format!("/posts/{id}"), false, "").await;
        assert_eq!(status, StatusCode::OK);
        // Reads join the stored author's name; writes use the verified token name.
        assert_eq!(
            read,
            format!(
                r#"{{"post":{{"id":{id},"body":"hello","created_at":"{created}","author":"stored-name","like_count":1}}}}"#
            )
        );
        let (status, feed) = request(&fixture, "GET", "/feed", false, "").await;
        assert_eq!(status, StatusCode::OK);
        let feed: Value = serde_json::from_str(&feed).unwrap();
        assert_eq!(feed["posts"][0]["id"], id);
        assert_eq!(feed["posts"][0]["like_count"], 1);
    }

    #[tokio::test]
    async fn routes_have_exact_errors_and_health_checks_sqlite() {
        let fixture = fixture();
        for id in ["0", "-1", "abc", "1.5", "+1", "%FF"] {
            assert_eq!(
                request(&fixture, "GET", &format!("/posts/{id}"), false, "").await,
                (
                    StatusCode::BAD_REQUEST,
                    r#"{"error":"invalid post id"}"#.to_owned()
                )
            );
        }
        assert_eq!(
            request(&fixture, "GET", "/posts/99999", false, "").await,
            (
                StatusCode::NOT_FOUND,
                r#"{"error":"post not found"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "POST", "/posts/99999/like", true, "").await,
            (
                StatusCode::NOT_FOUND,
                r#"{"error":"post not found"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "GET", "/unknown", false, "").await,
            (StatusCode::NOT_FOUND, r#"{"error":"not found"}"#.to_owned())
        );
        let (status, response) = request(&fixture, "GET", "/health", false, "").await;
        assert_eq!(status, StatusCode::OK);
        let health: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(
            response,
            format!(
                r#"{{"status":"ok","db":"ok","uptime_s":{}}}"#,
                health["uptime_s"]
            )
        );
    }

    #[tokio::test]
    async fn dispatch_preserves_method_allow_and_encoded_id_behavior() {
        let fixture = fixture();
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/feed")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get(ALLOW).unwrap(), "GET,HEAD");

        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/posts/1/like")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(response.headers().get(ALLOW).unwrap(), "POST");

        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let (status, body) = request(&fixture, "GET", "/posts/%31", false, "").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("seed post"));
        assert_eq!(
            request(&fixture, "GET", "/posts/%2F", false, "").await,
            (
                StatusCode::BAD_REQUEST,
                r#"{"error":"invalid post id"}"#.to_owned()
            )
        );
        assert_eq!(
            request(&fixture, "GET", "/posts/1/extra", false, "").await,
            (StatusCode::NOT_FOUND, r#"{"error":"not found"}"#.to_owned())
        );
        assert_eq!(
            request(&fixture, "GET", "/posts/1/", false, "").await,
            (StatusCode::NOT_FOUND, r#"{"error":"not found"}"#.to_owned())
        );
        let get_response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/posts/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!get_response.headers().contains_key(CONTENT_LENGTH));
        let get_length = get_response.body().size_hint().exact().unwrap();
        let head_response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/posts/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::OK);
        assert_eq!(
            head_response.headers()[CONTENT_LENGTH],
            get_length.to_string()
        );
        assert!(
            head_response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );

        let get_response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!get_response.headers().contains_key(CONTENT_LENGTH));
        let get_length = get_response.body().size_hint().exact().unwrap();
        let head_response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            head_response.headers()[CONTENT_LENGTH],
            get_length.to_string()
        );
        assert!(
            head_response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn http1_frames_responses_and_reuses_connection_after_head() {
        for read_batching in [false, true] {
            exercise_http1_connection(read_batching).await;
        }
    }

    async fn exercise_http1_connection(read_batching: bool) {
        let scratch = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
        std::fs::create_dir_all(&scratch).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("web-wire-")
            .tempdir_in(scratch)
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let service = hyper_router(
            seeded_db(&directory, read_batching),
            Auth::new(b"test-secret"),
        );
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    crate::server::serve(listener, service, async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                })
        });

        let responses = tokio::task::spawn_blocking(move || {
            let stream = TcpStream::connect(address).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut stream = BufReader::new(stream);

            stream
                .get_mut()
                .write_all(b"GET /posts/1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let post = read_http_response(&mut stream, true);

            stream
                .get_mut()
                .write_all(b"HEAD /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let missing_head = read_http_response(&mut stream, false);

            stream
                .get_mut()
                .write_all(b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .unwrap();
            let missing_get = read_http_response(&mut stream, true);
            (post, missing_head, missing_get)
        })
        .await
        .unwrap();

        let (post_response, missing_head, missing_get) = responses;
        assert_eq!(post_response.0, 200);
        assert_eq!(
            post_response.1["content-length"].parse::<usize>().unwrap(),
            post_response.2.len()
        );
        assert!(
            std::str::from_utf8(&post_response.2)
                .unwrap()
                .contains("seed post")
        );

        assert_eq!(missing_head.0, 404);
        assert_eq!(
            missing_head.1["content-length"],
            missing_get.1["content-length"]
        );
        assert_eq!(missing_head.1["content-length"], "21");

        assert_eq!(missing_get.0, 404);
        assert_eq!(
            missing_get.1["content-length"].parse::<usize>().unwrap(),
            missing_get.2.len()
        );
        assert_eq!(missing_get.2, br#"{"error":"not found"}"#);

        shutdown_tx.send(()).unwrap();
        server.join().unwrap().unwrap();
    }

    #[tokio::test]
    async fn dispatch_keeps_create_body_limit() {
        let fixture = fixture();
        for authenticated in [false, true] {
            let mut builder = Request::builder().method("POST").uri("/posts");
            if authenticated {
                builder = builder.header("Authorization", format!("Bearer {}", fixture.token));
            }
            let request = builder
                .body(Body::from(vec![b' '; 2 * 1024 * 1024 + 1]))
                .unwrap();
            let response = fixture.app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            assert!(response.headers().contains_key(CONTENT_TYPE));
            assert!(!response.headers().contains_key("authorization"));
            assert!(
                !response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .is_empty()
            );
        }
    }
}
