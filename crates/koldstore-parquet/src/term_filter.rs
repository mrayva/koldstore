//! Per-segment term filters for `text[]` and `jsonb` columns.
//!
//! Min/max bounds cannot describe an array or a document, so a containment predicate (`tags @> ...`,
//! `tags && ...`, `doc @> ...`, `doc ? ...`) can never rule a cold segment out through them. A term
//! filter does: the writer extracts *terms* from every value of the column (array elements; for jsonb
//! the keys, key/value pairs and array elements found at any depth), hashes the distinct terms into the
//! same one-hash bitmap the primary-key value summaries use, and stores it in
//! `cold_segment_index.value_summary` on the column's index row.
//!
//! A probe is a conjunction of *clauses*, each a disjunction of terms: a row can match only when, for
//! every clause, it holds at least one of the clause's terms. A segment is skipped when the bitmap shows
//! some clause has none of its terms in the segment. The terms are deliberately path-free (`{"a":{"b":1}}`
//! yields the terms for key `a`, key `b` and pair `b=1`), which keeps the test a *necessary* condition of
//! containment: it can keep a segment that holds no match, never skip one that does.
//!
//! The term encoding below is **frozen** (it is persisted inside the bitmaps, hashed with
//! `koldstore_sortkey::summary_hash`); changing it needs a new codec version.

use std::collections::HashSet;

use arrow_array::{Array, RecordBatch, StringArray};
use koldstore_sortkey::{summary_hash, ValueSummaryBuilder};
use serde_json::Value;

/// One probe: every clause must be satisfied, a clause is satisfied by any of its terms.
pub type TermClauses = Vec<Vec<Vec<u8>>>;

const TAG_ARRAY_ELEMENT: u8 = 0x01;
const TAG_JSON_KEY: u8 = 0x02;
const TAG_JSON_PAIR: u8 = 0x03;
const TAG_JSON_ELEMENT: u8 = 0x04;
const TAG_TSVECTOR_LEXEME: u8 = 0x05;

/// Which column type a filter is built for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermFilterKind {
    /// A one-dimensional `text[]`, stored as a PostgreSQL array literal (or a JSON array of strings).
    TextArray,
    /// A `jsonb` document stored as JSON text.
    Jsonb,
    /// A `tsvector`, stored as its text form (`'a':1A,2B 'b':3`).
    Tsvector,
}

/// The hash a term is stored under, as the `bigint` the catalog query compares against.
#[must_use]
pub fn term_hash(term: &[u8]) -> i64 {
    i64::try_from(summary_hash(term)).expect("summary_hash is below 2^63")
}

fn array_element_term(element: &str) -> Vec<u8> {
    let mut term = Vec::with_capacity(element.len() + 1);
    term.push(TAG_ARRAY_ELEMENT);
    term.extend_from_slice(element.as_bytes());
    term
}

fn lexeme_term(lexeme: &str) -> Vec<u8> {
    let mut term = Vec::with_capacity(lexeme.len() + 1);
    term.push(TAG_TSVECTOR_LEXEME);
    term.extend_from_slice(lexeme.as_bytes());
    term
}

/// Reads one single-quoted token (`''` and backslash escapes) starting at the opening quote; returns the
/// unescaped text and the index just past the closing quote.
fn read_quoted(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut out = String::new();
    let mut i = start + 1;
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                out.push(*chars.get(i + 1)?);
                i += 2;
            }
            '\'' if chars.get(i + 1) == Some(&'\'') => {
                out.push('\'');
                i += 2;
            }
            '\'' => return Some((out, i + 1)),
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    None
}

/// The lexemes of a `tsvector` text value. `None` when the text is not in the canonical output form.
#[must_use]
pub fn parse_tsvector_lexemes(text: &str) -> Option<Vec<String>> {
    let chars: Vec<char> = text.chars().collect();
    let mut lexemes = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            ' ' => i += 1,
            '\'' => {
                let (lexeme, next) = read_quoted(&chars, i)?;
                lexemes.push(lexeme);
                i = next;
                // Positions and weights (`:1A,2B`) hold no quotes or spaces.
                if chars.get(i) == Some(&':') {
                    while i < chars.len() && chars[i] != ' ' {
                        i += 1;
                    }
                }
            }
            _ => return None,
        }
    }
    Some(lexemes)
}

#[derive(Debug)]
enum QueryNode {
    /// A lexeme every matching document must contain.
    Lexeme(String),
    /// Prefix operands, `!` and anything else that proves nothing.
    Unknown,
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
}

struct QueryParser {
    chars: Vec<char>,
    pos: usize,
}

impl QueryParser {
    fn skip_ws(&mut self) {
        while self.chars.get(self.pos) == Some(&' ') {
            self.pos += 1;
        }
    }

    fn eat(&mut self, c: char) -> bool {
        self.skip_ws();
        if self.chars.get(self.pos) == Some(&c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn or(&mut self) -> Option<QueryNode> {
        let mut nodes = vec![self.and()?];
        while self.eat('|') {
            nodes.push(self.and()?);
        }
        Some(if nodes.len() == 1 { nodes.pop()? } else { QueryNode::Or(nodes) })
    }

    fn and(&mut self) -> Option<QueryNode> {
        let mut nodes = vec![self.phrase()?];
        while self.eat('&') {
            nodes.push(self.phrase()?);
        }
        Some(if nodes.len() == 1 { nodes.pop()? } else { QueryNode::And(nodes) })
    }

    /// `<->` and `<N>` both require the two operands, so they prune like `&`.
    fn phrase(&mut self) -> Option<QueryNode> {
        let mut nodes = vec![self.unary()?];
        loop {
            self.skip_ws();
            if self.chars.get(self.pos) != Some(&'<') {
                break;
            }
            while self.chars.get(self.pos).is_some_and(|c| *c != '>') {
                self.pos += 1;
            }
            self.pos += 1;
            nodes.push(self.unary()?);
        }
        Some(if nodes.len() == 1 { nodes.pop()? } else { QueryNode::And(nodes) })
    }

    fn unary(&mut self) -> Option<QueryNode> {
        if self.eat('!') {
            self.unary()?;
            return Some(QueryNode::Unknown);
        }
        if self.eat('(') {
            let node = self.or()?;
            return self.eat(')').then_some(node);
        }
        self.skip_ws();
        if self.chars.get(self.pos) != Some(&'\'') {
            return None;
        }
        let (lexeme, next) = read_quoted(&self.chars, self.pos)?;
        self.pos = next;
        let mut prefix = false;
        if self.chars.get(self.pos) == Some(&':') {
            self.pos += 1;
            while let Some(c) = self.chars.get(self.pos).copied() {
                match c {
                    '*' => prefix = true,
                    'A'..='D' | 'a'..='d' => {}
                    _ => break,
                }
                self.pos += 1;
            }
        }
        Some(if prefix { QueryNode::Unknown } else { QueryNode::Lexeme(lexeme) })
    }
}

/// Most clauses a `tsquery` may expand to; beyond it the probe proves nothing.
const MAX_QUERY_CLAUSES: usize = 64;

/// CNF of a node; `None` means "no constraint".
fn query_cnf(node: &QueryNode) -> Option<Option<TermClauses>> {
    match node {
        QueryNode::Unknown => Some(None),
        QueryNode::Lexeme(lexeme) => Some(Some(vec![vec![lexeme_term(lexeme)]])),
        QueryNode::And(children) => {
            let mut clauses = Vec::new();
            for child in children {
                if let Some(mut more) = query_cnf(child)? {
                    clauses.append(&mut more);
                }
            }
            (clauses.len() <= MAX_QUERY_CLAUSES).then_some(Some(clauses))
        }
        QueryNode::Or(children) => {
            let mut acc: TermClauses = Vec::new();
            for (index, child) in children.iter().enumerate() {
                // An unconstrained branch makes the whole disjunction unconstrained.
                let Some(child_clauses) = query_cnf(child)? else {
                    return Some(None);
                };
                if index == 0 {
                    acc = child_clauses;
                    continue;
                }
                let mut next = Vec::with_capacity(acc.len() * child_clauses.len());
                for left in &acc {
                    for right in &child_clauses {
                        let mut merged = left.clone();
                        merged.extend(right.iter().cloned());
                        next.push(merged);
                    }
                }
                if next.len() > MAX_QUERY_CLAUSES {
                    return None;
                }
                acc = next;
            }
            Some(Some(acc))
        }
    }
}

/// `tsv @@ <tsquery>`: the lexemes a matching document must contain, from the query's text form.
/// `None` when the query proves nothing (negation, prefixes, too many alternatives) or cannot be parsed.
#[must_use]
pub fn tsquery_clauses(text: &str) -> Option<TermClauses> {
    let mut parser = QueryParser {
        chars: text.chars().collect(),
        pos: 0,
    };
    let node = parser.or()?;
    parser.skip_ws();
    if parser.pos != parser.chars.len() {
        return None;
    }
    query_cnf(&node)?.filter(|clauses| !clauses.is_empty())
}

fn json_key_term(key: &str) -> Vec<u8> {
    let mut term = Vec::with_capacity(key.len() + 1);
    term.push(TAG_JSON_KEY);
    term.extend_from_slice(key.as_bytes());
    term
}

fn json_pair_term(key: &str, scalar: &[u8]) -> Vec<u8> {
    let mut term = Vec::with_capacity(key.len() + scalar.len() + 5);
    term.push(TAG_JSON_PAIR);
    term.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_be_bytes());
    term.extend_from_slice(key.as_bytes());
    term.extend_from_slice(scalar);
    term
}

fn json_element_term(scalar: &[u8]) -> Vec<u8> {
    let mut term = Vec::with_capacity(scalar.len() + 1);
    term.push(TAG_JSON_ELEMENT);
    term.extend_from_slice(scalar);
    term
}

/// Canonical bytes of a JSON scalar. Numbers go through `f64`, so numerically equal jsonb values
/// (`1`, `1.0`, `1e0`) share a term; distinct numbers that round to one `f64` merely share a bit.
fn scalar_canon(value: &Value) -> Option<Vec<u8>> {
    Some(match value {
        Value::String(text) => {
            let mut out = Vec::with_capacity(text.len() + 1);
            out.push(b's');
            out.extend_from_slice(text.as_bytes());
            out
        }
        Value::Number(number) => {
            let float = number.as_f64()?;
            let float = if float == 0.0 { 0.0 } else { float };
            let mut out = vec![b'n'];
            out.extend_from_slice(&float.to_bits().to_be_bytes());
            out
        }
        Value::Bool(true) => vec![b't'],
        Value::Bool(false) => vec![b'f'],
        Value::Null => vec![b'z'],
        Value::Array(_) | Value::Object(_) => return None,
    })
}

/// Calls `emit` with every term of one jsonb document. `false` when a number cannot be canonicalized.
fn emit_json_terms(value: &Value, top_level: bool, emit: &mut impl FnMut(Vec<u8>)) -> bool {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                emit(json_key_term(key));
                if let Some(scalar) = scalar_canon(child) {
                    emit(json_pair_term(key, &scalar));
                } else if !emit_json_terms(child, false, emit) {
                    return false;
                }
            }
            true
        }
        Value::Array(items) => {
            for item in items {
                if let Some(scalar) = scalar_canon(item) {
                    emit(json_element_term(&scalar));
                } else if !emit_json_terms(item, false, emit) {
                    return false;
                }
            }
            true
        }
        scalar => {
            // A top-level scalar behaves like a one-element array for `?` and `@>`.
            debug_assert!(top_level || scalar_canon(scalar).is_some());
            match scalar_canon(scalar) {
                Some(canon) => {
                    emit(json_element_term(&canon));
                    true
                }
                None => false,
            }
        }
    }
}

/// Drops the explicit bounds a non-default lower bound adds to an array literal (`[0:2]={x,y,z}`).
fn strip_dimension_prefix(text: &str) -> &str {
    let Some(rest) = text.strip_prefix('[') else {
        return text;
    };
    let Some((bounds, tail)) = rest.split_once("]=") else {
        return text;
    };
    let is_bound = |part: &str| {
        let digits = part.strip_prefix('-').unwrap_or(part);
        !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
    };
    match bounds.split_once(':') {
        Some((low, high)) if is_bound(low) && is_bound(high) => tail,
        _ => text,
    }
}

/// Parses a one-dimensional PostgreSQL text-array literal (`{a,"b c",NULL}`) or a JSON array.
#[must_use]
pub fn parse_text_array(text: &str) -> Option<Vec<Option<String>>> {
    let trimmed = strip_dimension_prefix(text.trim());
    if trimmed.starts_with('[') {
        let Value::Array(items) = serde_json::from_str::<Value>(trimmed).ok()? else {
            return None;
        };
        return Some(
            items
                .into_iter()
                .map(|item| match item {
                    Value::Null => None,
                    Value::String(text) => Some(text),
                    other => Some(other.to_string()),
                })
                .collect(),
        );
    }
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?;
    let mut items = Vec::new();
    let mut chars = inner.chars().peekable();
    if inner.trim().is_empty() {
        return Some(items);
    }
    loop {
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) {
            chars.next();
        }
        let mut value = String::new();
        let mut quoted = false;
        match chars.peek() {
            Some('{') => return None,
            Some('"') => {
                quoted = true;
                chars.next();
                loop {
                    match chars.next()? {
                        '\\' => value.push(chars.next()?),
                        '"' => break,
                        ch => value.push(ch),
                    }
                }
            }
            _ => {
                while let Some(&ch) = chars.peek() {
                    if ch == ',' {
                        break;
                    }
                    chars.next();
                    if ch == '\\' {
                        value.push(chars.next()?);
                    } else {
                        value.push(ch);
                    }
                }
                value = value.trim_end().to_string();
            }
        }
        while chars.peek().is_some_and(|ch| ch.is_whitespace()) {
            chars.next();
        }
        items.push(if !quoted && value.eq_ignore_ascii_case("null") {
            None
        } else {
            Some(value)
        });
        match chars.next() {
            None => return Some(items),
            Some(',') => {}
            Some(_) => return None,
        }
    }
}

/// Collects the distinct terms of a column across the batches of one segment.
#[derive(Debug, Clone)]
pub struct TermFilterBuilder {
    kind: TermFilterKind,
    hashes: HashSet<u64>,
    rows: usize,
    complete: bool,
}

impl TermFilterBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new(kind: TermFilterKind) -> Self {
        Self {
            kind,
            hashes: HashSet::new(),
            rows: 0,
            complete: true,
        }
    }

    /// Adds every value of `column` in `batch`. Anything unexpected disables the filter: a partial
    /// term set would cause false negatives.
    pub fn add_batch(&mut self, batch: &RecordBatch, column: &str) {
        if !self.complete {
            return;
        }
        let Some(array) = batch
            .column_by_name(column)
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        else {
            self.complete = false;
            return;
        };
        for index in 0..array.len() {
            self.rows += 1;
            if array.is_null(index) {
                continue;
            }
            if !self.add_value(array.value(index)) {
                self.complete = false;
                return;
            }
        }
    }

    fn add_value(&mut self, text: &str) -> bool {
        let hashes = &mut self.hashes;
        let mut emit = |term: Vec<u8>| {
            hashes.insert(summary_hash(&term));
        };
        match self.kind {
            TermFilterKind::TextArray => match parse_text_array(text) {
                Some(items) => {
                    for item in items.into_iter().flatten() {
                        emit(array_element_term(&item));
                    }
                    true
                }
                None => false,
            },
            TermFilterKind::Jsonb => match serde_json::from_str::<Value>(text) {
                Ok(value) => emit_json_terms(&value, true, &mut emit),
                Err(_) => false,
            },
            TermFilterKind::Tsvector => match parse_tsvector_lexemes(text) {
                Some(lexemes) => {
                    for lexeme in lexemes {
                        emit(lexeme_term(&lexeme));
                    }
                    true
                }
                None => false,
            },
        }
    }

    /// The bitmap, only if it covered exactly `row_count` rows and fits the catalog cap; `None` means
    /// the segment is simply never pruned by this column.
    #[must_use]
    pub fn finish(self, row_count: usize) -> Option<Vec<u8>> {
        if !self.complete || self.rows != row_count || self.hashes.is_empty() {
            return None;
        }
        let mut builder = ValueSummaryBuilder::new();
        for hash in self.hashes {
            builder.insert_hash(hash);
        }
        builder.finish()
    }
}

/// A term filter or sketch rebuilt from a segment object (see `rebuild_term_filter_from_parquet`).
#[derive(Debug)]
pub struct RebuiltColumnSummary {
    /// The encoded bitmap or sketch; `None` when the segment cannot carry one.
    pub summary: Option<Vec<u8>>,
    /// Rows read, to be checked against the catalog's row count.
    pub rows: usize,
}

/// Reads one column of a Parquet object batch by batch.
pub(crate) fn for_each_column_batch(
    data: impl Into<bytes::Bytes>,
    column: &str,
    mut visit: impl FnMut(&RecordBatch),
) -> Result<(), String> {
    use parquet::arrow::ProjectionMask;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(data.into())
        .map_err(|error| format!("cannot read parquet object: {error}"))?;
    let root = builder
        .schema()
        .index_of(column)
        .map_err(|_| format!("column `{column}` is not in the object"))?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [root]);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|error| format!("cannot read parquet object: {error}"))?;
    for batch in reader {
        visit(&batch.map_err(|error| format!("cannot read parquet object: {error}"))?);
    }
    Ok(())
}

/// Rebuilds the term filter flush would have written for `column` of a segment object that predates the
/// column's listing in `pruning_columns`. The result is a pure function of the (immutable) object.
///
/// # Errors
///
/// Returns an error when the object cannot be read or lacks the column.
pub fn rebuild_term_filter_from_parquet(
    data: impl Into<bytes::Bytes>,
    column: &str,
    kind: TermFilterKind,
) -> Result<RebuiltColumnSummary, String> {
    let mut builder = TermFilterBuilder::new(kind);
    let mut rows = 0_usize;
    for_each_column_batch(data, column, |batch| {
        rows += batch.num_rows();
        builder.add_batch(batch, column);
    })?;
    Ok(RebuiltColumnSummary {
        summary: builder.finish(rows),
        rows,
    })
}

/// `tags @> ARRAY[..]`: every element must be present. `None` when the probe proves nothing (empty
/// array) or can never match (a NULL element), in which case the segment is simply not pruned.
#[must_use]
pub fn array_contains_clauses(elements: &[Option<String>]) -> Option<TermClauses> {
    if elements.is_empty() || elements.iter().any(Option::is_none) {
        return None;
    }
    Some(
        elements
            .iter()
            .flatten()
            .map(|element| vec![array_element_term(element)])
            .collect(),
    )
}

/// `tags && ARRAY[..]`: at least one non-NULL element must be present.
#[must_use]
pub fn array_overlap_clauses(elements: &[Option<String>]) -> Option<TermClauses> {
    let terms: Vec<Vec<u8>> = elements
        .iter()
        .flatten()
        .map(|element| array_element_term(element))
        .collect();
    (!terms.is_empty()).then(|| vec![terms])
}

/// `doc @> '<json>'`. A top-level scalar probe is ambiguous (it also matches an array holding it), so it
/// proves nothing.
#[must_use]
pub fn jsonb_contains_clauses(json: &str) -> Option<TermClauses> {
    let value = serde_json::from_str::<Value>(json).ok()?;
    if !matches!(value, Value::Object(_) | Value::Array(_)) {
        return None;
    }
    let mut clauses = TermClauses::new();
    collect_probe_clauses(&value, &mut clauses)?;
    (!clauses.is_empty()).then_some(clauses)
}

fn collect_probe_clauses(value: &Value, clauses: &mut TermClauses) -> Option<()> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                clauses.push(vec![json_key_term(key)]);
                match scalar_canon(child) {
                    Some(scalar) => clauses.push(vec![json_pair_term(key, &scalar)]),
                    None => collect_probe_clauses(child, clauses)?,
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                match scalar_canon(item) {
                    Some(scalar) => clauses.push(vec![json_element_term(&scalar)]),
                    None => collect_probe_clauses(item, clauses)?,
                }
            }
        }
        _ => return None,
    }
    Some(())
}

/// `doc ? 'k'`, `doc ?| keys` (`all = false`) and `doc ?& keys` (`all = true`). A key exists as an object
/// key, or as a string element of a top-level array / a top-level string scalar.
#[must_use]
pub fn jsonb_exists_clauses(keys: &[String], all: bool) -> Option<TermClauses> {
    if keys.is_empty() {
        return None;
    }
    let terms = |key: &String| {
        let mut string = vec![b's'];
        string.extend_from_slice(key.as_bytes());
        vec![json_key_term(key), json_element_term(&string)]
    };
    if all {
        Some(keys.iter().map(terms).collect())
    } else {
        Some(vec![keys.iter().flat_map(terms).collect()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use koldstore_sortkey::summary_may_contain;
    use std::sync::Arc;

    fn batch(values: &[Option<&str>]) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("c", DataType::Utf8, true)]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn filter(kind: TermFilterKind, values: &[Option<&str>]) -> Vec<u8> {
        let mut builder = TermFilterBuilder::new(kind);
        builder.add_batch(&batch(values), "c");
        builder.finish(values.len()).expect("filter is built")
    }

    fn may_match(summary: &[u8], clauses: &TermClauses) -> bool {
        clauses.iter().all(|clause| {
            clause
                .iter()
                .any(|term| summary_may_contain(summary, term))
        })
    }

    #[test]
    fn tsvector_filter_and_tsquery_clauses() {
        let summary = filter(
            TermFilterKind::Tsvector,
            &[
                Some("'a':1A,2B 'b':3 'it''s':5 'back\\\\slash':6"),
                None,
                Some("'quoted lexeme':4 '日本語':1"),
                Some(""),
            ],
        );
        let probe = |q: &str| tsquery_clauses(q).map(|c| may_match(&summary, &c));
        assert_eq!(probe("'a' & 'b'"), Some(true));
        assert_eq!(probe("'it''s'"), Some(true));
        assert_eq!(probe("'quoted lexeme'"), Some(true));
        assert_eq!(probe("'日本語'"), Some(true));
        assert_eq!(probe("'back\\\\slash'"), Some(true));
        assert_eq!(probe("'a' <-> 'b'"), Some(true));
        assert_eq!(probe("'a' <3> 'b'"), Some(true));
        assert_eq!(probe("'a':A"), Some(true));
        assert_eq!(probe("('zzz' | 'b') & 'a'"), Some(true));
        // Mostly-empty summaries can still collide, so only assert absence for lexemes chosen to miss.
        assert_eq!(probe("'a' & 'zzzzzz-not-here'"), Some(false));
        assert_eq!(probe("'zzzzzz-not-here' | 'qqqqqq-not-here'"), Some(false));
        // Unconstrained shapes prove nothing.
        assert_eq!(probe("!'a'"), None);
        assert_eq!(probe("'a':*"), None);
        assert_eq!(probe("'a' | 'b':*"), None);
        assert_eq!(probe("'a' | !'b'"), None);
        assert_eq!(probe(""), None);
        assert_eq!(probe("'a' &"), None);
        // `!` under AND leaves the other side.
        assert_eq!(tsquery_clauses("'a' & !'b'").map(|c| c.len()), Some(1));
        // OR of ANDs distributes.
        assert_eq!(
            tsquery_clauses("('a' & 'b') | 'c'").map(|c| c.len()),
            Some(2)
        );
        assert_eq!(parse_tsvector_lexemes("'a' junk"), None);
    }

    #[test]
    fn term_encoding_is_frozen() {
        assert_eq!(lexeme_term("w"), vec![5, b'w']);
        assert_eq!(term_hash(&array_element_term("x")), term_hash(&[1, b'x']));
        assert_eq!(json_key_term("k"), vec![2, b'k']);
        assert_eq!(json_element_term(b"t"), vec![4, b't']);
        assert_eq!(json_pair_term("k", b"t"), vec![3, 0, 0, 0, 1, b'k', b't']);
    }

    #[test]
    fn parses_array_literals_and_json_arrays() {
        let some = |text: &str| Some(text.to_string());
        assert_eq!(parse_text_array("{}"), Some(vec![]));
        assert_eq!(
            parse_text_array(r#"{"a\"b","c\\d",NULL,"",x y,"NULL"}"#),
            Some(vec![some("a\"b"), some("c\\d"), None, some(""), some("x y"), some("NULL")])
        );
        assert_eq!(
            parse_text_array(r#"["alpha", null, "b"]"#),
            Some(vec![some("alpha"), None, some("b")])
        );
        assert_eq!(parse_text_array("{{1,2},{3,4}}"), None);
        assert_eq!(
            parse_text_array("[0:2]={x,y,z}"),
            Some(vec![Some("x".into()), Some("y".into()), Some("z".into())])
        );
        assert_eq!(
            parse_text_array("[-3:-2]={a,NULL}"),
            Some(vec![Some("a".into()), None])
        );
        assert_eq!(parse_text_array("garbage"), None);
    }

    #[test]
    fn array_filter_has_no_false_negatives() {
        let summary = filter(
            TermFilterKind::TextArray,
            &[Some("{a,b}"), None, Some(r#"{"c d",NULL}"#), Some("{}")],
        );
        let present = |e: &str| array_contains_clauses(&[Some(e.to_string())]).unwrap();
        for element in ["a", "b", "c d"] {
            assert!(may_match(&summary, &present(element)), "{element}");
        }
        let overlap = array_overlap_clauses(&[Some("zz".into()), Some("b".into())]).unwrap();
        assert!(may_match(&summary, &overlap));
        let absent = array_overlap_clauses(&[Some("definitely-absent-1".into())]).unwrap();
        assert!(!may_match(&summary, &absent));
        assert!(array_contains_clauses(&[Some("a".into()), None]).is_none());
        assert!(array_contains_clauses(&[]).is_none());
        assert!(array_overlap_clauses(&[None]).is_none());
    }

    #[test]
    fn jsonb_filter_has_no_false_negatives() {
        let docs = [
            r#"{"k": 3, "flag": true}"#,
            r#"{"a": {"b": [1, {"c": "x"}]}}"#,
            r#"["red", "green"]"#,
            r#""solo""#,
            r#"{"n": 1.0}"#,
        ];
        let summary = filter(TermFilterKind::Jsonb, &docs.map(Some));
        for probe in [
            r#"{"k": 3}"#,
            r#"{"flag": true}"#,
            r#"{"a": {"b": [1]}}"#,
            r#"{"a": {"b": [{"c": "x"}]}}"#,
            r#"["green"]"#,
            r#"{"n": 1}"#,
            r#"{"n": 1.00}"#,
        ] {
            let clauses = jsonb_contains_clauses(probe).unwrap();
            assert!(may_match(&summary, &clauses), "{probe}");
        }
        for key in ["k", "flag", "a", "b", "c", "red", "solo"] {
            let clauses = jsonb_exists_clauses(&[key.to_string()], false).unwrap();
            assert!(may_match(&summary, &clauses), "?{key}");
        }
        let absent = jsonb_contains_clauses(r#"{"nope-absent": 99}"#).unwrap();
        assert!(!may_match(&summary, &absent));
        let absent_key = jsonb_exists_clauses(&["nope-absent".into()], false).unwrap();
        assert!(!may_match(&summary, &absent_key));
        assert!(jsonb_contains_clauses("5").is_none());
        assert!(jsonb_contains_clauses("{}").is_none());
    }

    #[test]
    fn an_unparseable_or_miscounted_column_gets_no_filter() {
        let mut builder = TermFilterBuilder::new(TermFilterKind::Jsonb);
        builder.add_batch(&batch(&[Some("{\"a\":1}"), Some("not json")]), "c");
        assert!(builder.finish(2).is_none());
        let mut builder = TermFilterBuilder::new(TermFilterKind::TextArray);
        builder.add_batch(&batch(&[Some("{a}")]), "c");
        assert!(builder.clone().finish(2).is_none(), "row count mismatch");
        assert!(builder.finish(1).is_some());
        let mut builder = TermFilterBuilder::new(TermFilterKind::TextArray);
        builder.add_batch(&batch(&[None, Some("{}")]), "c");
        assert!(builder.finish(2).is_none(), "no terms at all");
    }
}
