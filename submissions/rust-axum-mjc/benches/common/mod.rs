use std::hint::black_box;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rusqlite::{Connection, params};
use serde_json::json;
use tempfile::TempDir;
use tokio::runtime::{Builder, Runtime};
use twelve_dollar_server::{auth::Auth, db::Db};

const SECRET: &str = "twelve-dollar-challenge";
const POSTS: i64 = 1_000;
const LIKES_PER_POST: i64 = 16;

pub struct AuthFixture {
    auth: Auth,
    headers: HeaderMap,
}

impl AuthFixture {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let claims = json!({
            "sub": "1", "username": "user1", "iat": now, "exp": now + 86_400,
        });
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        Self {
            auth: Auth::new(SECRET),
            headers,
        }
    }

    // Every invocation verifies the signature and expiry using production auth.
    pub fn verify(&self) {
        let user = self.auth.authenticate(black_box(&self.headers)).unwrap();
        assert_eq!(user.user_id, 1);
        assert_eq!(user.username, "user1");
        black_box(user);
    }
}

pub struct DbFixture {
    db: Db,
    runtime: Runtime,
    // Keep database files alive until the DB handle and runtime have dropped.
    _directory: TempDir,
}

impl DbFixture {
    pub fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/bench-fixtures");
        std::fs::create_dir_all(&root).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("sqlite-")
            .tempdir_in(root)
            .unwrap();
        let path = directory.path().join("feed.db");
        let mut connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../../../schema.sql"))
            .unwrap();
        {
            let transaction = connection.transaction().unwrap();
            for user in 1..=64 {
                transaction
                    .execute(
                        "INSERT INTO users (id, username) VALUES (?1, ?2)",
                        params![user, format!("user{user}")],
                    )
                    .unwrap();
            }
            for post in 1..=POSTS {
                // Ten posts share each timestamp, exercising the id tie-breaker.
                let minute = (post - 1) / 10;
                let timestamp = format!("2025-01-01T{:02}:{:02}:00.000Z", minute / 60, minute % 60);
                transaction
                    .execute(
                        "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![post, (post - 1) % 64 + 1, "a".repeat(200), timestamp],
                    )
                    .unwrap();
                for user in 1..=LIKES_PER_POST {
                    transaction
                        .execute(
                            "INSERT INTO likes (user_id, post_id) VALUES (?1, ?2)",
                            params![user, post],
                        )
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
        }
        drop(connection);
        let fixture = Self {
            db: Db::open(path).unwrap(),
            runtime: Builder::new_current_thread().enable_all().build().unwrap(),
            _directory: directory,
        };
        // Prepare/cache statements and verify the fixture before measurement.
        fixture.health();
        let feed = fixture.runtime.block_on(fixture.db.feed()).unwrap();
        let feed: serde_json::Value = serde_json::from_slice(&feed).unwrap();
        let posts = feed["posts"].as_array().unwrap();
        assert_eq!(posts.len(), 20);
        for (offset, post) in posts.iter().enumerate() {
            assert_eq!(post["id"].as_i64(), Some(POSTS - offset as i64));
            assert_eq!(post["like_count"].as_i64(), Some(LIKES_PER_POST));
            assert_eq!(post["body"].as_str(), Some("a".repeat(200).as_str()));
        }
        let post = fixture
            .runtime
            .block_on(fixture.db.post(POSTS))
            .unwrap()
            .unwrap();
        let post: serde_json::Value = serde_json::from_slice(&post).unwrap();
        assert_eq!(post["post"]["id"].as_i64(), Some(POSTS));
        assert_eq!(post["post"]["like_count"].as_i64(), Some(LIKES_PER_POST));
        assert_eq!(post["post"]["author"].as_str(), Some("user40"));
        fixture.duplicate_like();
        fixture
    }

    pub fn health(&self) {
        self.runtime.block_on(self.db.health()).unwrap();
    }

    pub fn feed(&self) {
        let response = self.runtime.block_on(self.db.feed()).unwrap();
        assert!(!response.is_empty());
        black_box(response);
    }

    pub fn post(&self) {
        let response = self
            .runtime
            .block_on(self.db.post(black_box(POSTS)))
            .unwrap()
            .unwrap();
        assert!(!response.is_empty());
        black_box(response);
    }

    // Repeated requests preserve fixture cardinality; no growing write benchmark.
    pub fn duplicate_like(&self) {
        let already_liked = self
            .runtime
            .block_on(self.db.like(black_box(1), black_box(POSTS)))
            .unwrap();
        assert_eq!(already_liked, Some(true));
        black_box(already_liked);
    }
}
