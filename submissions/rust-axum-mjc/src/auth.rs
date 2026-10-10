use axum::http::{HeaderMap, header::AUTHORIZATION};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, errors::ErrorKind};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::Value;
use std::fmt;
use std::marker::PhantomData;
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

#[derive(Debug)]
struct Claims<U> {
    exp: Option<u64>,
    sub: Option<i64>,
    username: U,
}

impl<'de, U: Deserialize<'de> + Default> Deserialize<'de> for Claims<U> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ClaimsVisitor<U>(PhantomData<U>);
        impl<'de, U: Deserialize<'de> + Default> Visitor<'de> for ClaimsVisitor<U> {
            type Value = Claims<U>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JWT claims object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut exp = None;
                let mut sub = None;
                let mut username = U::default();
                let mut seen_exp = false;
                let mut seen_sub = false;
                let mut seen_username = false;

                while let Some(key) = map.next_key::<ClaimKey>()? {
                    match key {
                        ClaimKey::Exp => {
                            if std::mem::replace(&mut seen_exp, true) {
                                return Err(de::Error::duplicate_field("exp"));
                            }
                            exp = map.next_value::<LooseExp>()?.0;
                        }
                        ClaimKey::Sub => {
                            if std::mem::replace(&mut seen_sub, true) {
                                return Err(de::Error::duplicate_field("sub"));
                            }
                            sub = map.next_value::<LooseSubject>()?.0;
                        }
                        ClaimKey::Username => {
                            if std::mem::replace(&mut seen_username, true) {
                                return Err(de::Error::duplicate_field("username"));
                            }
                            username = map.next_value()?;
                        }
                        ClaimKey::Other => {
                            map.next_value::<CheckedIgnored>()?;
                        }
                    }
                }

                Ok(Claims { exp, sub, username })
            }
        }

        d.deserialize_struct(
            "Claims",
            &["exp", "sub", "username"],
            ClaimsVisitor(PhantomData),
        )
    }
}

#[derive(Deserialize)]
#[serde(field_identifier)]
#[serde(rename_all = "lowercase")]
enum ClaimKey {
    Exp,
    Sub,
    Username,
    #[serde(other)]
    Other,
}

trait ClaimValue: Default {
    fn from_u64(_: u64) -> Option<Self> {
        None
    }

    fn from_str(_: &str) -> Option<Self> {
        None
    }

    fn from_string(value: String) -> Option<Self> {
        Self::from_str(&value)
    }
}

struct ClaimValueVisitor<T>(PhantomData<T>);
impl<'de, T: ClaimValue> Visitor<'de> for ClaimValueVisitor<T> {
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JWT claim value")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(T::from_u64(value).unwrap_or_default())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(T::default())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(T::default())
    }

    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(T::default())
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
        Ok(T::from_str(value).unwrap_or_default())
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(T::from_str(value).unwrap_or_default())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(T::from_string(value).unwrap_or_default())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(T::default())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(T::default())
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        while seq.next_element::<CheckedIgnored>()?.is_some() {}
        Ok(T::default())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        while map
            .next_entry::<CheckedIgnored, CheckedIgnored>()?
            .is_some()
        {}
        Ok(T::default())
    }
}

fn deserialize_claim_value<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: ClaimValue,
{
    d.deserialize_any(ClaimValueVisitor(PhantomData))
}

#[derive(Default)]
struct LooseExp(Option<u64>);
impl ClaimValue for LooseExp {
    fn from_u64(value: u64) -> Option<Self> {
        Some(Self(Some(value)))
    }
}
impl<'de> Deserialize<'de> for LooseExp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_claim_value(d)
    }
}

#[derive(Default)]
struct LooseSubject(Option<i64>);
impl ClaimValue for LooseSubject {
    fn from_str(value: &str) -> Option<Self> {
        Some(Self(positive_id(value)))
    }
}
impl<'de> Deserialize<'de> for LooseSubject {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_claim_value(d)
    }
}

#[derive(Default)]
struct UsernameOwned(Option<String>);
impl ClaimValue for UsernameOwned {
    fn from_str(value: &str) -> Option<Self> {
        Some(Self(Some(value.to_owned())))
    }

    fn from_string(value: String) -> Option<Self> {
        Some(Self(Some(value)))
    }
}
impl<'de> Deserialize<'de> for UsernameOwned {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_claim_value(d)
    }
}

#[derive(Default)]
struct UsernamePresent(bool);
impl ClaimValue for UsernamePresent {
    fn from_str(_: &str) -> Option<Self> {
        Some(Self(true))
    }

    fn from_string(_: String) -> Option<Self> {
        Some(Self(true))
    }
}
impl<'de> Deserialize<'de> for UsernamePresent {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_claim_value(d)
    }
}

#[derive(Default)]
struct CheckedIgnored;
impl ClaimValue for CheckedIgnored {}
impl<'de> Deserialize<'de> for CheckedIgnored {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        deserialize_claim_value(d)
    }
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
        let token = bearer(headers)?;
        let claims = decode_claims::<UsernameOwned>(token, &self.key, &self.validation)?;
        let user_id = validate_exp_and_subject(claims.exp, claims.sub)?;
        let username = claims.username.0.ok_or("invalid token payload")?;
        Ok(User { user_id, username })
    }

    pub fn authenticate_id(&self, headers: &HeaderMap) -> Result<i64, &'static str> {
        let token = bearer(headers)?;
        let claims = decode_claims::<UsernamePresent>(token, &self.key, &self.validation)?;
        let user_id = validate_exp_and_subject(claims.exp, claims.sub)?;
        if !claims.username.0 {
            return Err("invalid token payload");
        }
        Ok(user_id)
    }
}

fn bearer(headers: &HeaderMap) -> Result<&[u8], &'static str> {
    let token = headers
        .get(AUTHORIZATION)
        .and_then(|header| header.as_bytes().strip_prefix(b"Bearer "))
        .ok_or("missing bearer token")?;
    Ok(token)
}

fn decode_claims<U: for<'de> Deserialize<'de> + Default>(
    token: &[u8],
    key: &DecodingKey,
    validation: &Validation,
) -> Result<Claims<U>, &'static str> {
    let claims = match decode::<Claims<U>>(token, key, validation) {
        Ok(data) => data.claims,
        Err(typed_error) => {
            match typed_error.kind() {
                ErrorKind::Json(_) => {}
                ErrorKind::InvalidClaimFormat(claim) | ErrorKind::MissingRequiredClaim(claim)
                    if claim == "sub" =>
                {
                    return Err("invalid token payload");
                }
                _ => return Err("invalid or expired token"),
            }

            match decode::<Value>(token, key, validation) {
                Ok(data) => claims_from_value(data.claims),
                Err(value_error) => match value_error.kind() {
                    ErrorKind::Json(_) => {
                        // jsonwebtoken's secondary registered-claim parser can reject a
                        // structured subject. Recover only that unusual verified case.
                        let value = verified_claims(token, key)
                            .filter(|claims| claims.get("sub").is_some_and(|sub| !sub.is_string()))
                            .ok_or("invalid or expired token")?;
                        claims_from_value(value)
                    }
                    ErrorKind::InvalidClaimFormat(claim)
                    | ErrorKind::MissingRequiredClaim(claim)
                        if claim == "sub" =>
                    {
                        return Err("invalid token payload");
                    }
                    _ => {
                        return Err("invalid or expired token");
                    }
                },
            }
        }
    };
    Ok(claims)
}

fn claims_from_value<U: for<'de> Deserialize<'de> + Default>(mut value: Value) -> Claims<U> {
    let exp = value.get("exp").and_then(Value::as_u64);
    let sub = value
        .get("sub")
        .and_then(Value::as_str)
        .and_then(positive_id);
    let username = value
        .as_object_mut()
        .and_then(|claims| claims.remove("username"))
        .unwrap_or(Value::Null);
    Claims {
        exp,
        sub,
        username: serde_json::from_value(username).unwrap_or_default(),
    }
}

fn validate_exp_and_subject(exp: Option<u64>, sub: Option<i64>) -> Result<i64, &'static str> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "invalid or expired token")?
        .as_secs();
    let expires = exp.ok_or("invalid or expired token")?;
    if expires <= now {
        return Err("invalid or expired token");
    }
    let user_id = sub.ok_or("invalid token payload")?;
    Ok(user_id)
}

fn verified_claims(token: &[u8], key: &DecodingKey) -> Option<Value> {
    let header = jsonwebtoken::decode_header(token).ok()?;
    if header.alg != Algorithm::HS256 {
        return None;
    }
    let first_dot = token.iter().position(|byte| *byte == b'.')?;
    let second_dot = token[first_dot + 1..]
        .iter()
        .position(|byte| *byte == b'.')?
        + first_dot
        + 1;
    if token[second_dot + 1..].contains(&b'.') {
        return None;
    }
    let signature = std::str::from_utf8(&token[second_dot + 1..]).ok()?;
    if !jsonwebtoken::crypto::verify(signature, &token[..second_dot], key, Algorithm::HS256).ok()? {
        return None;
    }
    let claims = jsonwebtoken::dangerous::insecure_decode::<Value>(token)
        .ok()?
        .claims;
    Some(claims)
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
    use serde::ser::SerializeMap;
    use serde_json::json;

    struct DuplicateClaim(&'static str);
    impl serde::Serialize for DuplicateClaim {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let exp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600;
            let mut map = serializer.serialize_map(Some(4))?;
            map.serialize_entry("exp", &exp)?;
            map.serialize_entry("sub", "1")?;
            map.serialize_entry("username", "first")?;
            match self.0 {
                "username" => map.serialize_entry("username", "last")?,
                "exp" => map.serialize_entry("exp", &0u64)?,
                "sub" => map.serialize_entry("sub", &2u64)?,
                _ => unreachable!(),
            }
            map.end()
        }
    }

    fn authenticate_duplicate(key: &'static str) -> Result<User, &'static str> {
        let token = encode(
            &Header::new(Algorithm::HS256),
            &DuplicateClaim(key),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        Auth::new(b"test-secret").authenticate(&headers)
    }

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

    fn authenticate_id(
        payload: Value,
        algorithm: Algorithm,
        secret: &[u8],
    ) -> Result<i64, &'static str> {
        let token = encode(
            &Header::new(algorithm),
            &payload,
            &EncodingKey::from_secret(secret),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        Auth::new(b"test-secret").authenticate_id(&headers)
    }

    fn authenticate_raw(payload: &str) -> Result<User, &'static str> {
        authenticate_token(&token_with_payload(payload.as_bytes()))
    }

    fn authenticate_token(token: &str) -> Result<User, &'static str> {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        Auth::new(b"test-secret").authenticate(&headers)
    }

    fn token_with_payload(payload: &[u8]) -> String {
        const BASE64URL: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut encoded_payload = String::with_capacity(payload.len().div_ceil(3) * 4);
        for chunk in payload.chunks(3) {
            let a = chunk[0];
            let b = *chunk.get(1).unwrap_or(&0);
            let c = *chunk.get(2).unwrap_or(&0);
            encoded_payload.push(BASE64URL[(a >> 2) as usize] as char);
            encoded_payload.push(BASE64URL[(((a & 0b11) << 4) | (b >> 4)) as usize] as char);
            if chunk.len() > 1 {
                encoded_payload.push(BASE64URL[(((b & 0b1111) << 2) | (c >> 6)) as usize] as char);
            }
            if chunk.len() > 2 {
                encoded_payload.push(BASE64URL[(c & 0b11_1111) as usize] as char);
            }
        }

        let key = EncodingKey::from_secret(b"test-secret");
        let header = encode(&Header::new(Algorithm::HS256), &json!({}), &key)
            .unwrap()
            .split('.')
            .next()
            .unwrap()
            .to_owned();
        let signing_input = format!("{header}.{encoded_payload}");
        let signature =
            jsonwebtoken::crypto::sign(signing_input.as_bytes(), &key, Algorithm::HS256).unwrap();
        format!("{signing_input}.{signature}")
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
            let result = authenticate(payload, Algorithm::HS256, b"test-secret");
            assert_eq!(result, Err("invalid or expired token"));
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
            json!({"id":"1"}),
            json!(["1"]),
            json!(1),
            json!("0"),
            json!("-1"),
            json!("+1"),
            json!("1.5"),
            json!("abc"),
            json!(null),
        ] {
            let mut payload = claims();
            let subject_label = subject.to_string();
            payload["sub"] = subject;
            assert_eq!(
                authenticate(payload, Algorithm::HS256, b"test-secret"),
                Err("invalid token payload"),
                "subject={subject_label}"
            );
        }
        let mut expired_with_structured_subject = claims();
        expired_with_structured_subject["sub"] = json!({"id":"1"});
        expired_with_structured_subject["exp"] = json!(0);
        assert_eq!(
            authenticate(
                expired_with_structured_subject.clone(),
                Algorithm::HS256,
                b"test-secret"
            ),
            Err("invalid or expired token")
        );
        let mut invalid_signature_subject = claims();
        invalid_signature_subject["sub"] = json!({"id":"1"});
        assert_eq!(
            authenticate(invalid_signature_subject, Algorithm::HS256, b"wrong-secret"),
            Err("invalid or expired token")
        );
        let mut payload = claims();
        payload.as_object_mut().unwrap().remove("sub");
        assert_eq!(
            authenticate(payload, Algorithm::HS256, b"test-secret"),
            Err("invalid token payload")
        );
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

    #[test]
    fn authenticate_id_matches_user_authentication_without_retaining_username() {
        let mut escaped = claims();
        escaped["username"] = json!("line\nbreak-\"quoted\"");
        let mut invalid_exp = claims();
        invalid_exp["exp"] = json!("tomorrow");
        let mut invalid_sub = claims();
        invalid_sub["sub"] = json!("+1");
        let mut invalid_username = claims();
        invalid_username["username"] = json!(false);
        let mut structured_sub = claims();
        structured_sub["sub"] = json!({"id": "1"});

        for payload in [
            claims(),
            escaped,
            invalid_exp,
            invalid_sub,
            invalid_username,
            structured_sub.clone(),
        ] {
            let user_result = authenticate(payload.clone(), Algorithm::HS256, b"test-secret")
                .map(|user| user.user_id);
            assert_eq!(
                authenticate_id(payload, Algorithm::HS256, b"test-secret"),
                user_result
            );
        }
        assert_eq!(
            authenticate_id(structured_sub, Algorithm::HS256, b"wrong-secret"),
            Err("invalid or expired token")
        );
    }

    #[test]
    fn typed_decode_fallback_preserves_duplicate_claim_value_semantics() {
        assert_eq!(
            authenticate_duplicate("username"),
            Ok(User {
                user_id: 1,
                username: "last".to_owned()
            })
        );
        assert_eq!(
            authenticate_duplicate("exp"),
            Err("invalid or expired token")
        );
        assert_eq!(authenticate_duplicate("sub"), Err("invalid token payload"));
    }

    #[test]
    fn rejects_invalid_unicode_in_ignored_claim_values_and_keys() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        for payload in [
            format!(r#"{{"exp":{exp},"sub":"1","username":"user","ignored":"\ud800"}}"#),
            format!(
                r#"{{"exp":{exp},"sub":"1","username":"user","ignored":{{"nested":["\ud800"]}}}}"#
            ),
            format!(
                r#"{{"exp":{exp},"sub":"1","username":"user","ignored":{{"\ud800":"value"}}}}"#
            ),
            format!(r#"{{"exp":{exp},"sub":"1","username":["\ud800"]}}"#),
        ] {
            assert_eq!(authenticate_raw(&payload), Err("invalid or expired token"));
        }
    }

    #[test]
    fn rejects_invalid_utf8_bytes_in_claims_for_both_auth_paths() {
        let exp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        let common = format!(r#"{{"exp":{exp},"sub":"1","username":"user","#);
        let cases = [
            (format!(r#"{common}"ignored":""#), "\"}".to_owned()),
            (
                format!(r#"{common}"ignored":{{"nested":[""#),
                "\"]}}".to_owned(),
            ),
            (
                format!(r#"{common}"ignored":{{""#),
                "\":\"value\"}}".to_owned(),
            ),
            (
                format!(r#"{{"exp":{exp},"sub":"1","username":[""#),
                "\"]}".to_owned(),
            ),
            (
                "{\"exp\":{\"nested\":\"".to_owned(),
                r#""},"sub":"1","username":"user"}"#.to_owned(),
            ),
            (
                format!(r#"{{"exp":{exp},"sub":{{"nested":""#),
                r#""},"username":"user"}"#.to_owned(),
            ),
            (
                format!(r#"{{"exp":{exp},"sub":"1","username":"user",""#),
                r#"":"value"}"#.to_owned(),
            ),
        ];

        for invalid_utf8 in [&[0xff][..], &[0xc3, 0x28][..]] {
            let payloads = cases.iter().map(|(prefix, suffix)| {
                let mut valid_payload = prefix.as_bytes().to_vec();
                valid_payload.push(b'x');
                valid_payload.extend_from_slice(suffix.as_bytes());
                serde_json::from_slice::<Value>(&valid_payload).unwrap();

                let mut payload = prefix.as_bytes().to_vec();
                payload.extend_from_slice(invalid_utf8);
                payload.extend_from_slice(suffix.as_bytes());
                payload
            });

            for payload in payloads {
                let token = token_with_payload(&payload);
                let mut headers = HeaderMap::new();
                headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
                let auth = Auth::new(b"test-secret");
                assert_eq!(auth.authenticate(&headers), Err("invalid or expired token"));
                assert_eq!(
                    auth.authenticate_id(&headers),
                    Err("invalid or expired token")
                );
            }
        }
    }
}
