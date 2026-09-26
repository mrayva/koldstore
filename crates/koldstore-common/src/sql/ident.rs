//! PostgreSQL identifier validation and quoting.
//!
//! Any non-empty name without a NUL byte is a legal PostgreSQL identifier once
//! it is double-quoted, so generated SQL always goes through [`quote_ident`].
//! [`is_safe_identifier`] only says whether a name needs no quoting at all.

/// Returns true when `value` is a plain unquoted SQL identifier (ASCII letters,
/// digits and `_`, not starting with a digit).
#[must_use]
pub fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

/// Returns true when `value` can be used as a (quoted) identifier: non-empty and
/// free of NUL bytes. (PostgreSQL itself truncates over-long names at 63 bytes.)
#[must_use]
pub fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}

/// Quotes an identifier, doubling any embedded double quote.
///
/// # Panics
///
/// Panics when `value` is not a valid identifier (empty or holding a NUL byte).
#[must_use]
pub fn quote_ident(value: &str) -> String {
    assert!(
        is_valid_identifier(value),
        "quote_ident requires a valid identifier: {value:?}"
    );
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Renders an identifier the way `regclass::text` does: bare when it is a plain
/// identifier, double-quoted otherwise.
#[must_use]
pub fn display_ident(value: &str) -> String {
    if is_safe_identifier(value) {
        value.to_string()
    } else {
        format!("\"{}\"", value.replace('"', "\"\""))
    }
}

/// Escapes a SQL string literal body for single-quoted PostgreSQL text.
#[must_use]
pub fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Splits dotted identifier text into its parts, honouring double-quoted parts
/// (`""` is an escaped quote, a dot inside quotes does not split). An unquoted
/// part is taken as typed. Returns `None` for empty or malformed text.
#[must_use]
pub fn split_qualified(value: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = value.chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next();
            loop {
                match chars.next()? {
                    '"' if chars.peek() == Some(&'"') => {
                        chars.next();
                        part.push('"');
                    }
                    '"' => break,
                    character => part.push(character),
                }
            }
        } else {
            while let Some(&character) = chars.peek() {
                if character == '.' {
                    break;
                }
                part.push(character);
                chars.next();
            }
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') => {}
            Some(_) => return None,
        }
    }
}

/// Quotes a dotted PostgreSQL identifier path (each part may itself be
/// double-quoted in `value`).
///
/// # Panics
///
/// Panics when `value` is malformed or a part is not a valid identifier.
#[must_use]
pub fn quote_qualified_ident(value: &str) -> String {
    split_qualified(value)
        .unwrap_or_else(|| panic!("quote_qualified_ident requires a well-formed name: {value:?}"))
        .iter()
        .map(|part| quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

/// Percent-encodes every byte outside `[A-Za-z0-9_-]`, so a schema or table name
/// containing `/`, `..`, spaces or non-ASCII letters cannot escape or split its
/// own object prefix. Plain names -- the only ones older releases accepted -- are
/// unchanged, so existing prefixes stay valid.
#[must_use]
pub fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-' {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// Stable 64-bit FNV-1a: identical across releases and platforms, so names
/// derived from it can be found again later.
#[must_use]
pub fn stable_name_hash(value: &str) -> u64 {
    value.as_bytes().iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// The stem helper objects (mirror table, guard triggers and functions) derive
/// their names from: `schema_name` for plain names, exactly as before. When a
/// part is not a plain identifier the stem is a sanitized form plus a hash of the
/// exact (schema, name) pair, so derived names stay plain ASCII and two distinct
/// source names never share a stem (`"a b"` and `"a_b"` differ).
#[must_use]
pub fn derived_base_name(schema: Option<&str>, name: &str) -> String {
    let plain = is_safe_identifier(name) && schema.is_none_or(is_safe_identifier);
    let joined = schema.map_or_else(|| name.to_string(), |schema| format!("{schema}_{name}"));
    if plain {
        return joined;
    }
    let sanitized: String = joined
        .chars()
        .map(|character| if character.is_ascii_alphanumeric() { character } else { '_' })
        .collect();
    let leading = if sanitized.starts_with(|c: char| c.is_ascii_digit()) { "_" } else { "" };
    let hash = stable_name_hash(&format!("{}\0{name}", schema.unwrap_or("")));
    format!("{leading}{sanitized}_{hash:016x}")
}

/// `value` cut to at most `max_bytes`, never inside a UTF-8 codepoint.
#[must_use]
pub fn floor_str(value: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
