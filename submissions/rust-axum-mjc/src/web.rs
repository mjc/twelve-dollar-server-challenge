use crate::{
    auth::{Auth, positive_id},
    db::{Db, DbError},
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State, rejection::PathRejection},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, sync::Arc, time::Instant};

struct AppState {
    db: Db,
    auth: Auth,
    started: Instant,
}

pub fn router(db: Db, auth: Auth) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/feed", get(feed))
        .route("/posts", post(create_post))
        .route("/posts/{id}", get(get_post))
        .route("/posts/{id}/like", post(like_post))
        .fallback(|| async { error(StatusCode::NOT_FOUND, "not found") })
        .with_state(Arc::new(AppState {
            db,
            auth,
            started: Instant::now(),
        }))
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(ErrorBody { error: message })).into_response()
}

fn encoded_json(status: StatusCode, body: Vec<u8>) -> Response {
    (status, [(CONTENT_TYPE, "application/json")], body).into_response()
}

fn internal_error(error_value: DbError) -> Response {
    eprintln!("database request failed: {error_value}");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
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

async fn health(State(state): State<Arc<AppState>>) -> Response {
    match state.db.health().await {
        Ok(()) => Json(HealthBody {
            status: "ok",
            db: "ok",
            uptime_s: state.started.elapsed().as_secs(),
        })
        .into_response(),
        Err(error_value) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(DegradedHealth {
                status: "degraded",
                db: "unreachable",
                error: error_value.to_string(),
            }),
        )
            .into_response(),
    }
}

async fn feed(State(state): State<Arc<AppState>>) -> Response {
    match state.db.feed().await {
        Ok(body) => encoded_json(StatusCode::OK, body),
        Err(error_value) => internal_error(error_value),
    }
}

async fn get_post(
    State(state): State<Arc<AppState>>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Some(id) = path.ok().and_then(|Path(id)| positive_id(&id)) else {
        return error(StatusCode::BAD_REQUEST, "invalid post id");
    };
    match state.db.post(id).await {
        Ok(Some(body)) => encoded_json(StatusCode::OK, body),
        Ok(None) => error(StatusCode::NOT_FOUND, "post not found"),
        Err(error_value) => internal_error(error_value),
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
    // Validate the entire document first, so malformed JSON remains distinct
    // from a valid document with a missing or incorrectly typed body field.
    let raw: &RawValue = serde_json::from_slice(bytes).map_err(|_| "malformed JSON body")?;
    if !raw.get().starts_with('{') {
        return Err("body is required");
    }
    let incoming: Incoming<'_> = serde_json::from_str(raw.get()).map_err(|error| {
        if error.is_data() {
            "body is required"
        } else {
            "malformed JSON body"
        }
    })?;
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
    headers: HeaderMap,
    bytes: Bytes,
) -> Response {
    let user = match state.auth.authenticate(&headers) {
        Ok(user) => user,
        Err(message) => return error(StatusCode::UNAUTHORIZED, message),
    };
    let body = match post_body(&bytes) {
        Ok(body) => body,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    match state
        .db
        .create_post(user.user_id, body, user.username)
        .await
    {
        Ok(body) => encoded_json(StatusCode::CREATED, body),
        Err(error_value) => internal_error(error_value),
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
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
) -> Response {
    let user = match state.auth.authenticate(&headers) {
        Ok(user) => user,
        Err(message) => return error(StatusCode::UNAUTHORIZED, message),
    };
    let Some(post_id) = path.ok().and_then(|Path(id)| positive_id(&id)) else {
        return error(StatusCode::BAD_REQUEST, "invalid post id");
    };
    match state.db.like(user.user_id, post_id).await {
        Ok(Some(already_liked)) => (
            if already_liked {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            Json(LikeBody {
                liked: true,
                already_liked,
                post_id,
            }),
        )
            .into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "post not found"),
        Err(error_value) => internal_error(error_value),
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
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tempfile::TempDir;
    use tower::ServiceExt;

    struct Fixture {
        app: Router,
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

    fn fixture() -> Fixture {
        let scratch = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
        std::fs::create_dir_all(&scratch).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("web-")
            .tempdir_in(scratch)
            .unwrap();
        let path = directory.path().join("feed.db");
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../../schema.sql"))
            .unwrap();
        connection.execute_batch("INSERT INTO users (id, username) VALUES (1, 'stored-name'), (2, 'other-user'); INSERT INTO posts (id,user_id,body,created_at) VALUES (1,1,'seed post','2000-01-01T00:00:00.000Z');").unwrap();
        drop(connection);
        let db = Db::open(&path).unwrap();
        let token = encode(&Header::new(Algorithm::HS256), &json!({
            "sub":"1", "username":"token-name", "exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600,
        }), &EncodingKey::from_secret(b"test-secret")).unwrap();
        Fixture {
            app: router(db, Auth::new(b"test-secret")),
            token,
            _directory: directory,
        }
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
}
