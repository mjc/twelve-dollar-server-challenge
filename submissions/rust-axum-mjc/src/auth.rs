use axum::http::{HeaderMap, header::AUTHORIZATION};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct Auth {
    key: DecodingKey,
    validation: Validation,
}

#[derive(Debug, PartialEq, Eq)]
pub struct User {
    pub user_id: i64,
    pub username: String,
}

impl Auth {
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        // Check expiration ourselves so exp == now is expired and claim type errors
        // have the same response as expired tokens. No clock-skew allowance.
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        Self {
            key: DecodingKey::from_secret(secret.as_ref()),
            validation,
        }
    }

    pub fn authenticate(&self, headers: &HeaderMap) -> Result<User, &'static str> {
        let token = headers
            .get(AUTHORIZATION)
            .and_then(|header| header.as_bytes().strip_prefix(b"Bearer "))
            .ok_or("missing bearer token")?;
        let mut claims = decode::<Value>(token, &self.key, &self.validation)
            .map_err(|_| "invalid or expired token")?
            .claims;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "invalid or expired token")?
            .as_secs();
        let expires = claims
            .get("exp")
            .and_then(Value::as_u64)
            .ok_or("invalid or expired token")?;
        if expires <= now {
            return Err("invalid or expired token");
        }
        let user_id = claims
            .get("sub")
            .and_then(Value::as_str)
            .and_then(positive_id)
            .ok_or("invalid token payload")?;
        let username = match claims.get_mut("username").map(Value::take) {
            Some(Value::String(username)) => username,
            _ => return Err("invalid token payload"),
        };
        Ok(User { user_id, username })
    }
}

pub(crate) fn positive_id(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse::<i64>().ok().filter(|id| *id > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;

    fn authenticate(
        payload: Value,
        algorithm: Algorithm,
        secret: &[u8],
    ) -> Result<User, &'static str> {
        let token = encode(
            &Header::new(algorithm),
            &payload,
            &EncodingKey::from_secret(secret),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        Auth::new(b"test-secret").authenticate(&headers)
    }

    fn claims() -> Value {
        json!({"sub":"1", "username":"token-name", "exp":SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600})
    }

    #[test]
    fn verifies_every_signature_and_only_accepts_hs256() {
        assert_eq!(
            authenticate(claims(), Algorithm::HS256, b"test-secret").unwrap(),
            User {
                user_id: 1,
                username: "token-name".to_owned()
            }
        );
        assert_eq!(
            authenticate(claims(), Algorithm::HS256, b"wrong-secret"),
            Err("invalid or expired token")
        );
        assert_eq!(
            authenticate(claims(), Algorithm::HS384, b"test-secret"),
            Err("invalid or expired token")
        );
        let auth = Auth::new(b"test-secret");
        let mut headers = HeaderMap::new();
        assert_eq!(auth.authenticate(&headers), Err("missing bearer token"));
        headers.insert(AUTHORIZATION, "bearer abc".parse().unwrap());
        assert_eq!(auth.authenticate(&headers), Err("missing bearer token"));
        headers.insert(AUTHORIZATION, "Bearer abc".parse().unwrap());
        assert_eq!(auth.authenticate(&headers), Err("invalid or expired token"));
        headers.insert(
            AUTHORIZATION,
            axum::http::HeaderValue::from_bytes("Bearer é".as_bytes()).unwrap(),
        );
        assert_eq!(auth.authenticate(&headers), Err("invalid or expired token"));
    }

    #[test]
    fn expiration_is_required_and_has_no_leeway() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for expiration in [
            json!(now),
            json!(now - 1),
            json!("9999999999"),
            json!(null),
            json!(-1),
        ] {
            let mut payload = claims();
            payload["exp"] = expiration;
            assert_eq!(
                authenticate(payload, Algorithm::HS256, b"test-secret"),
                Err("invalid or expired token")
            );
        }
        let mut payload = claims();
        payload.as_object_mut().unwrap().remove("exp");
        assert_eq!(
            authenticate(payload, Algorithm::HS256, b"test-secret"),
            Err("invalid or expired token")
        );
    }

    #[test]
    fn valid_signatures_with_invalid_user_claims_have_payload_errors() {
        for subject in [
            json!(1),
            json!("0"),
            json!("-1"),
            json!("+1"),
            json!("1.5"),
            json!("abc"),
            json!(null),
        ] {
            let mut payload = claims();
            payload["sub"] = subject;
            assert_eq!(
                authenticate(payload, Algorithm::HS256, b"test-secret"),
                Err("invalid token payload")
            );
        }
        for username in [json!(1), json!(null)] {
            let mut payload = claims();
            payload["username"] = username;
            assert_eq!(
                authenticate(payload, Algorithm::HS256, b"test-secret"),
                Err("invalid token payload")
            );
        }
        let mut payload = claims();
        payload.as_object_mut().unwrap().remove("username");
        assert_eq!(
            authenticate(payload, Algorithm::HS256, b"test-secret"),
            Err("invalid token payload")
        );
    }
}
