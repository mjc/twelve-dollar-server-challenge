use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Post<'a> {
    pub id: i64,
    pub body: &'a str,
    pub created_at: &'a str,
    pub author: &'a str,
    pub like_count: i64,
}

impl Post<'_> {
    pub fn write_json(&self, output: &mut Vec<u8>) -> serde_json::Result<()> {
        output.extend_from_slice(b"{\"id\":");
        serde_json::to_writer(&mut *output, &self.id)?;
        output.extend_from_slice(b",\"body\":");
        serde_json::to_writer(&mut *output, self.body)?;
        output.extend_from_slice(b",\"created_at\":");
        serde_json::to_writer(&mut *output, self.created_at)?;
        output.extend_from_slice(b",\"author\":");
        serde_json::to_writer(&mut *output, self.author)?;
        output.extend_from_slice(b",\"like_count\":");
        serde_json::to_writer(&mut *output, &self.like_count)?;
        output.push(b'}');
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Post;

    #[test]
    fn fixed_key_writer_matches_derived_json_bytes() {
        let posts = [
            Post {
                id: i64::MIN,
                body: "quotes: \" slash: \\ controls: \0\n\r\t\u{0001} unicode: 雪🙂",
                created_at: "2026-10-10T19:45:12.345Z",
                author: "a\"uthor\\\n雪",
                like_count: i64::MAX,
            },
            Post {
                id: i64::MAX,
                body: "",
                created_at: "",
                author: "",
                like_count: i64::MIN,
            },
        ];

        for post in posts {
            let expected = serde_json::to_vec(&post).unwrap();
            let mut actual = Vec::new();
            post.write_json(&mut actual).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
