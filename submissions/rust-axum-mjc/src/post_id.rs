/// Parse a positive decimal post id from one raw URI path segment.
///
/// Percent escapes are decoded one byte at a time, without allocating. The
/// decoded value must consist only of ASCII decimal digits.
pub(crate) fn parse_post_id(raw_segment: &str) -> Option<i64> {
    let bytes = raw_segment.as_bytes();
    let mut index = 0;
    let mut value = 0_i64;
    let mut found_digit = false;

    while index < bytes.len() {
        let byte = if bytes[index] == b'%' {
            let high = hex(*bytes.get(index + 1)?)?;
            let low = hex(*bytes.get(index + 2)?)?;
            index += 3;
            (high << 4) | low
        } else {
            let byte = bytes[index];
            index += 1;
            byte
        };

        if !byte.is_ascii_digit() {
            return None;
        }
        found_digit = true;
        value = value.checked_mul(10)?.checked_add(i64::from(byte - b'0'))?;
    }

    (found_digit && value > 0).then_some(value)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_post_id;

    #[test]
    fn parses_digits_after_a_single_percent_decode() {
        assert_eq!(parse_post_id("%31%32"), Some(12));
        assert_eq!(parse_post_id("00012"), Some(12));
        assert_eq!(parse_post_id("9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn rejects_non_digits_malformed_escapes_and_overflow() {
        for value in [
            "",
            "0",
            "-1",
            "+1",
            " 1",
            "1 ",
            "%2F",
            "%",
            "%G1",
            "%2531",
            "%C3%A9",
            "9223372036854775808",
        ] {
            assert_eq!(parse_post_id(value), None, "{value}");
        }
    }
}
