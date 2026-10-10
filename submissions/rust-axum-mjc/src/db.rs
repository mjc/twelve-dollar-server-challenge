use std::{error::Error, fmt, path::Path, time::Duration};

use bytes::Bytes;
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use tokio::sync::{mpsc, oneshot};

use crate::model::Post;

const POST_QUERY: &str = "SELECT p.id, p.body, p.created_at, u.username,
    (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
    FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?1";
const FEED_QUERY: &str = "SELECT p.id, p.body, p.created_at, u.username,
    (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
    FROM posts p JOIN users u ON u.id = p.user_id
    ORDER BY p.created_at DESC, p.id DESC LIMIT 20";

#[derive(Debug)]
pub enum DbError {
    Sqlite(rusqlite::Error),
    Serialize(serde_json::Error),
    InvalidUtf8(std::str::Utf8Error),
    WorkerStart(std::io::Error),
    WorkerUnavailable,
    InvalidPath,
    WalUnavailable,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(error) => write!(f, "SQLite: {error}"),
            Self::Serialize(error) => write!(f, "JSON serialization: {error}"),
            Self::InvalidUtf8(error) => write!(f, "invalid post body UTF-8: {error}"),
            Self::WorkerStart(error) => write!(f, "database worker: {error}"),
            Self::WorkerUnavailable => f.write_str("database worker unavailable"),
            Self::InvalidPath => f.write_str("database must be an existing on-disk file"),
            Self::WalUnavailable => f.write_str("database did not enable WAL mode"),
        }
    }
}

impl Error for DbError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sqlite(error) => Some(error),
            Self::Serialize(error) => Some(error),
            Self::InvalidUtf8(error) => Some(error),
            Self::WorkerStart(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialize(error)
    }
}

impl From<std::str::Utf8Error> for DbError {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(error)
    }
}

type Reply<T> = oneshot::Sender<Result<T, DbError>>;

enum Request {
    Health(Reply<()>),
    Feed(Reply<Vec<u8>>),
    Post(i64, Reply<Option<Vec<u8>>>),
    CreatePost {
        user_id: i64,
        body: Bytes,
        username: String,
        reply: Reply<Vec<u8>>,
    },
    Like {
        user_id: i64,
        post_id: i64,
        reply: Reply<Option<bool>>,
    },
}

#[derive(Clone)]
pub struct Db {
    requests: mpsc::Sender<Request>,
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(DbError::InvalidPath);
        }
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_secs(5))?;
        let journal_mode: String =
            connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::WalUnavailable);
        }
        connection.pragma_update(None, "synchronous", "NORMAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;

        // Bound queued work; SQLite keeps its default page cache and statement cache.
        let (requests, mut receiver) = mpsc::channel(256);
        std::thread::Builder::new()
            .name("sqlite".into())
            .spawn(move || {
                while let Some(request) = receiver.blocking_recv() {
                    match request {
                        Request::Health(reply) => {
                            let result = connection
                                .prepare_cached("SELECT 1")
                                .and_then(|mut statement| statement.query_row([], |_| Ok(())))
                                .map_err(DbError::from);
                            let _ = reply.send(result);
                        }
                        Request::Feed(reply) => {
                            let _ = reply.send(read_feed(&connection));
                        }
                        Request::Post(id, reply) => {
                            let _ = reply.send(read_post(&connection, id));
                        }
                        Request::CreatePost {
                            user_id,
                            body,
                            username,
                            reply,
                        } => {
                            let _ =
                                reply.send(insert_post(&mut connection, user_id, body, username));
                        }
                        Request::Like {
                            user_id,
                            post_id,
                            reply,
                        } => {
                            let _ = reply.send(insert_like(&mut connection, user_id, post_id));
                        }
                    }
                }
            })
            .map_err(DbError::WorkerStart)?;
        Ok(Self { requests })
    }

    async fn request<T>(&self, make: impl FnOnce(Reply<T>) -> Request) -> Result<T, DbError> {
        let (reply, receiver) = oneshot::channel();
        self.requests
            .send(make(reply))
            .await
            .map_err(|_| DbError::WorkerUnavailable)?;
        receiver.await.map_err(|_| DbError::WorkerUnavailable)?
    }

    pub async fn health(&self) -> Result<(), DbError> {
        self.request(Request::Health).await
    }

    pub async fn feed(&self) -> Result<Vec<u8>, DbError> {
        self.request(Request::Feed).await
    }

    pub async fn post(&self, id: i64) -> Result<Option<Vec<u8>>, DbError> {
        self.request(|reply| Request::Post(id, reply)).await
    }

    pub async fn create_post(
        &self,
        user_id: i64,
        body: Bytes,
        username: String,
    ) -> Result<Vec<u8>, DbError> {
        self.request(|reply| Request::CreatePost {
            user_id,
            body,
            username,
            reply,
        })
        .await
    }

    pub async fn like(&self, user_id: i64, post_id: i64) -> Result<Option<bool>, DbError> {
        self.request(|reply| Request::Like {
            user_id,
            post_id,
            reply,
        })
        .await
    }
}

fn decode_post<'a>(row: &'a rusqlite::Row<'_>) -> rusqlite::Result<Post<'a>> {
    Ok(Post {
        id: row.get(0)?,
        body: row.get_ref(1)?.as_str()?,
        created_at: row.get_ref(2)?.as_str()?,
        author: row.get_ref(3)?.as_str()?,
        like_count: row.get(4)?,
    })
}

fn read_feed(connection: &Connection) -> Result<Vec<u8>, DbError> {
    let mut statement = connection.prepare_cached(FEED_QUERY)?;
    let mut rows = statement.query([])?;
    let mut output = Vec::from(b"{\"posts\":[".as_slice());
    let mut first = true;
    while let Some(row) = rows.next()? {
        if !first {
            output.push(b',');
        }
        first = false;
        decode_post(row)?.write_json(&mut output)?;
    }
    output.extend_from_slice(b"]}");
    Ok(output)
}

fn read_post(connection: &Connection, id: i64) -> Result<Option<Vec<u8>>, DbError> {
    let mut statement = connection.prepare_cached(POST_QUERY)?;
    let mut rows = statement.query([id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let mut output = Vec::from(b"{\"post\":".as_slice());
    decode_post(row)?.write_json(&mut output)?;
    output.push(b'}');
    Ok(Some(output))
}

fn insert_post(
    connection: &mut Connection,
    user_id: i64,
    body: Bytes,
    username: String,
) -> Result<Vec<u8>, DbError> {
    let body_text = std::str::from_utf8(&body)?;
    let transaction = connection.transaction()?;
    let output = {
        let mut statement = transaction.prepare_cached(
            "INSERT INTO posts (user_id, body) VALUES (?1, ?2) RETURNING id, created_at",
        )?;
        let mut rows = statement.query(params![user_id, body_text])?;
        let row = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let post = Post {
            id: row.get(0)?,
            body: body_text,
            created_at: row.get_ref(1)?.as_str().map_err(rusqlite::Error::from)?,
            author: &username,
            like_count: 0,
        };
        let mut output = Vec::from(b"{\"post\":".as_slice());
        post.write_json(&mut output)?;
        output.push(b'}');
        // Complete RETURNING before committing; borrowed row data is no longer used.
        rows.next()?;
        output
    };
    transaction.commit()?;
    Ok(output)
}

fn insert_like(
    connection: &mut Connection,
    user_id: i64,
    post_id: i64,
) -> Result<Option<bool>, DbError> {
    // Reserve the writer before the existence check so another writer cannot delete
    // the post between that check and the insert.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let exists: bool = transaction
        .prepare_cached("SELECT EXISTS (SELECT 1 FROM posts WHERE id = ?1)")?
        .query_row([post_id], |row| row.get(0))?;
    if !exists {
        return Ok(None);
    }
    let inserted = transaction
        .prepare_cached("INSERT INTO likes (user_id, post_id) VALUES (?1, ?2) ON CONFLICT (user_id, post_id) DO NOTHING")?
        .execute(params![user_id, post_id])?;
    transaction.commit()?;
    Ok(Some(inserted == 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(bytes: Vec<u8>) -> serde_json::Value {
        serde_json::from_slice(&bytes).unwrap()
    }

    fn test_directory() -> tempfile::TempDir {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/test-data");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::tempdir_in(root).unwrap()
    }

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
        let directory = test_directory();
        let path = directory.path().join("feed.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(include_str!("../../../schema.sql"))
            .unwrap();
        connection
            .execute(
                "INSERT INTO users (id, username) VALUES (1, 'alice'), (2, 'bob')",
                [],
            )
            .unwrap();
        (directory, path)
    }

    #[tokio::test]
    async fn committed_posts_preserve_utf8_and_token_author_and_survive_reopen() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let body = "héllo 🦀\n\"quoted\"";
        let created = json(
            db.create_post(1, body.into(), "token-name".into())
                .await
                .unwrap(),
        );
        let created = &created["post"];
        let id = created["id"].as_i64().unwrap();
        assert_eq!(created["body"], body);
        assert_eq!(created["author"], "token-name");
        assert_eq!(created["like_count"], 0);
        let timestamp = created["created_at"].as_str().unwrap();
        assert_eq!(timestamp.len(), 24);
        assert!(timestamp.ends_with('Z'));
        assert_eq!(db.like(2, id).await.unwrap(), Some(false));
        drop(db);

        let reopened = Db::open(&path).unwrap();
        let fetched = json(reopened.post(id).await.unwrap().unwrap());
        let fetched = &fetched["post"];
        assert_eq!(fetched["body"], body);
        assert_eq!(fetched["created_at"], timestamp);
        assert_eq!(fetched["author"], "alice");
        assert_eq!(fetched["like_count"], 1);
    }

    #[tokio::test]
    async fn duplicate_likes_are_atomic_and_each_read_gets_current_data() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        let post = json(
            db.create_post(1, "body".into(), "alice".into())
                .await
                .unwrap(),
        );
        let id = post["post"]["id"].as_i64().unwrap();
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            0
        );
        let (first, second) = tokio::join!(db.like(2, id), db.like(2, id));
        assert_eq!((first.unwrap(), second.unwrap()), (Some(false), Some(true)));
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            1
        );
        assert_eq!(json(db.feed().await.unwrap())["posts"][0]["like_count"], 1);

        let external = Connection::open(&path).unwrap();
        external
            .execute("INSERT INTO likes (user_id, post_id) VALUES (1, ?1)", [id])
            .unwrap();
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            2
        );
        assert_eq!(json(db.feed().await.unwrap())["posts"][0]["like_count"], 2);
        assert_eq!(db.like(1, id + 1).await.unwrap(), None);
        assert!(db.post(id + 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn feed_orders_timestamp_then_id_and_limits_to_twenty() {
        let (_directory, path) = fixture();
        let connection = Connection::open(&path).unwrap();
        for id in 1..=22 {
            connection.execute(
                "INSERT INTO posts (id, user_id, body, created_at) VALUES (?1, 1, 'body', '2025-01-01T00:00:00.000Z')",
                [id],
            ).unwrap();
        }
        connection
            .execute(
                "UPDATE posts SET created_at = '2026-01-01T00:00:00.000Z' WHERE id = 1",
                [],
            )
            .unwrap();
        let feed = json(Db::open(&path).unwrap().feed().await.unwrap());
        let feed = feed["posts"].as_array().unwrap();
        assert_eq!(feed.len(), 20);
        assert_eq!(
            feed.iter()
                .map(|post| post["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            std::iter::once(1).chain((4..=22).rev()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn sqlite_constraints_fail_without_poisoning_the_worker() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        assert!(
            db.create_post(999, "body".into(), "nobody".into())
                .await
                .is_err()
        );
        assert!(
            db.create_post(1, Bytes::new(), "alice".into())
                .await
                .is_err()
        );
        assert!(matches!(
            db.create_post(1, Bytes::from_static(&[0xff]), "alice".into())
                .await,
            Err(DbError::InvalidUtf8(_))
        ));
        let post = json(
            db.create_post(1, "body".into(), "alice".into())
                .await
                .unwrap(),
        );
        let id = post["post"]["id"].as_i64().unwrap();
        assert!(db.like(999, id).await.is_err());
        assert_eq!(
            json(db.post(id).await.unwrap().unwrap())["post"]["like_count"],
            0
        );
        db.health().await.unwrap();
    }

    #[tokio::test]
    async fn borrowed_rows_produce_exact_compact_json_and_escape_strings() {
        let (_directory, path) = fixture();
        let db = Db::open(&path).unwrap();
        assert_eq!(db.feed().await.unwrap(), br#"{"posts":[]}"#);
        let connection = Connection::open(&path).unwrap();
        connection.execute(
            "INSERT INTO posts (id, user_id, body, created_at) VALUES (1, 1, ?1, '2025-01-01T00:00:00.000Z')",
            ["héllo 🦀\n\"quoted\"\\\t"],
        ).unwrap();
        let post = db.post(1).await.unwrap().unwrap();
        assert_eq!(post, r#"{"post":{"id":1,"body":"héllo 🦀\n\"quoted\"\\\t","created_at":"2025-01-01T00:00:00.000Z","author":"alice","like_count":0}}"#.as_bytes());
        let feed = db.feed().await.unwrap();
        assert_eq!(feed, r#"{"posts":[{"id":1,"body":"héllo 🦀\n\"quoted\"\\\t","created_at":"2025-01-01T00:00:00.000Z","author":"alice","like_count":0}]}"#.as_bytes());
    }

    #[test]
    fn open_requires_existing_disk_database() {
        let directory = test_directory();
        let missing = directory.path().join("missing.db");
        assert!(Db::open(&missing).is_err());
        assert!(!missing.exists());
        assert!(matches!(Db::open(":memory:"), Err(DbError::InvalidPath)));
        assert!(matches!(Db::open(""), Err(DbError::InvalidPath)));
    }

    #[tokio::test]
    async fn closed_worker_returns_an_error() {
        let (requests, receiver) = mpsc::channel(1);
        drop(receiver);
        let db = Db { requests };
        assert!(matches!(db.health().await, Err(DbError::WorkerUnavailable)));
    }
}
