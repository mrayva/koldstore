//! Restricted row predicate for `koldstore_move_when`.
//!
//! A predicate is a conjunction of simple column terms. It is parsed once at
//! configuration time and persisted as typed data (column, operator, literal
//! text), never as SQL text, so the flush path compiles it from a fixed
//! operator set with quoted identifiers and escaped literals.
//!
//! Grammar (case-insensitive keywords):
//!
//! ```text
//! predicate := term { AND term }
//! term      := column cmp literal
//!            | column IN ( literal { , literal } )
//!            | column IS [NOT] NULL
//! cmp       := = | <> | != | < | <= | > | >=
//! literal   := 'text' | number | TRUE | FALSE
//! ```
//!
//! Anything else (OR, NOT, functions such as `now()`, subqueries, casts,
//! arithmetic) is rejected: such an expression could change its answer for an
//! unchanged row, which the flush sequence-fence reasoning relies on never
//! happening.

use serde::{Deserialize, Serialize};

/// Most terms one predicate may hold.
pub const MAX_PREDICATE_TERMS: usize = 16;
/// Most literals one `IN` list may hold.
pub const MAX_PREDICATE_IN_VALUES: usize = 100;

/// Comparison operator of one predicate term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    IsNull,
    IsNotNull,
}

impl PredicateOp {
    fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::In => "IN",
            Self::IsNull => "IS NULL",
            Self::IsNotNull => "IS NOT NULL",
        }
    }
}

/// One `column op literal(s)` term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredicateTerm {
    /// Column name (unquoted).
    pub column: String,
    /// Comparison operator.
    pub op: PredicateOp,
    /// Literal text; empty for `IS [NOT] NULL`, one entry for comparisons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

/// Conjunction of [`PredicateTerm`]s.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MovePredicate {
    pub terms: Vec<PredicateTerm>,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    if value.contains('\\') {
        format!("E'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

impl MovePredicate {
    /// Column names referenced by the predicate, without duplicates.
    #[must_use]
    pub fn columns(&self) -> Vec<&str> {
        let mut columns: Vec<&str> = Vec::new();
        for term in &self.terms {
            if !columns.contains(&term.column.as_str()) {
                columns.push(&term.column);
            }
        }
        columns
    }

    /// Checks the structural invariants of a decoded predicate.
    ///
    /// # Errors
    ///
    /// Returns an error when the term list is empty or too long, or a term's
    /// literal count does not match its operator.
    pub fn validate(&self) -> Result<(), String> {
        if self.terms.is_empty() || self.terms.len() > MAX_PREDICATE_TERMS {
            return Err(format!(
                "move_when must have between 1 and {MAX_PREDICATE_TERMS} AND-ed terms"
            ));
        }
        for term in &self.terms {
            if term.column.is_empty() {
                return Err("move_when column name must not be empty".into());
            }
            let ok = match term.op {
                PredicateOp::IsNull | PredicateOp::IsNotNull => term.values.is_empty(),
                PredicateOp::In => {
                    !term.values.is_empty() && term.values.len() <= MAX_PREDICATE_IN_VALUES
                }
                _ => term.values.len() == 1,
            };
            if !ok {
                return Err(format!(
                    "move_when term on column \"{}\" has the wrong number of literals",
                    term.column
                ));
            }
        }
        Ok(())
    }

    /// Compiles the predicate to a boolean SQL expression over `alias`.
    ///
    /// Literals are emitted as untyped string constants, so PostgreSQL coerces
    /// each to its column's type (and rejects an invalid one at parse time).
    #[must_use]
    pub fn to_sql(&self, alias: &str) -> String {
        self.render(Some(alias))
    }

    fn render(&self, alias: Option<&str>) -> String {
        let terms = self
            .terms
            .iter()
            .map(|term| {
                let column = match alias {
                    Some(alias) => format!("{alias}.{}", quote_ident(&term.column)),
                    None => quote_ident(&term.column),
                };
                match term.op {
                    PredicateOp::IsNull | PredicateOp::IsNotNull => {
                        format!("{column} {}", term.op.sql())
                    }
                    PredicateOp::In => {
                        let list = term
                            .values
                            .iter()
                            .map(|value| quote_literal(value))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("{column} IN ({list})")
                    }
                    _ => format!("{column} {} {}", term.op.sql(), quote_literal(&term.values[0])),
                }
            })
            .collect::<Vec<_>>();
        terms.join(" AND ")
    }

    /// Human-readable normalized form, for status output and messages.
    #[must_use]
    pub fn display(&self) -> String {
        self.render(None)
    }

    /// Parses the restricted grammar.
    ///
    /// # Errors
    ///
    /// Returns a message naming the unsupported construct or syntax problem.
    pub fn parse(input: &str) -> Result<Self, String> {
        let tokens = tokenize(input)?;
        let mut parser = Parser { tokens, pos: 0 };
        let mut terms = vec![parser.term()?];
        while parser.keyword("AND") {
            terms.push(parser.term()?);
        }
        if let Some(token) = parser.peek() {
            return Err(unsupported(token));
        }
        let predicate = Self { terms };
        predicate.validate()?;
        Ok(predicate)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Quoted(String),
    Str(String),
    Num(String),
    Cmp(&'static str),
    LParen,
    RParen,
    Comma,
}

fn unsupported(token: &Token) -> String {
    let what = match token {
        Token::Ident(word) => word.to_ascii_uppercase(),
        Token::Quoted(word) | Token::Str(word) | Token::Num(word) => word.clone(),
        Token::Cmp(op) => (*op).to_string(),
        Token::LParen => "(".into(),
        Token::RParen => ")".into(),
        Token::Comma => ",".into(),
    };
    format!(
        "unsupported construct `{what}` in move_when; only `column <op> constant`, \
         `column IN (constants)`, `column IS [NOT] NULL` joined by AND are allowed \
         (no OR, NOT, functions, casts or subqueries)"
    )
}

fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            c if c.is_whitespace() => i += 1,
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '\'' | '"' => {
                let quote = c;
                let mut text = String::new();
                i += 1;
                loop {
                    match chars.get(i) {
                        None => return Err("unterminated quoted value in move_when".into()),
                        Some(&ch) if ch == quote => {
                            if chars.get(i + 1) == Some(&quote) {
                                text.push(quote);
                                i += 2;
                            } else {
                                i += 1;
                                break;
                            }
                        }
                        Some(&ch) => {
                            text.push(ch);
                            i += 1;
                        }
                    }
                }
                tokens.push(if quote == '\'' {
                    Token::Str(text)
                } else {
                    Token::Quoted(text)
                });
            }
            '=' => {
                tokens.push(Token::Cmp("="));
                i += 1;
            }
            '<' | '>' | '!' => {
                let next = chars.get(i + 1).copied();
                let (op, width) = match (c, next) {
                    ('<', Some('=')) => ("<=", 2),
                    ('<', Some('>')) => ("<>", 2),
                    ('>', Some('=')) => (">=", 2),
                    ('!', Some('=')) => ("<>", 2),
                    ('<', _) => ("<", 1),
                    ('>', _) => (">", 1),
                    _ => return Err("unexpected `!` in move_when".into()),
                };
                tokens.push(Token::Cmp(op));
                i += width;
            }
            c if c.is_ascii_digit() || ((c == '-' || c == '+') && next_is_digit(&chars, i)) => {
                let start = i;
                i += 1;
                while i < chars.len()
                    && (chars[i].is_ascii_digit()
                        || chars[i] == '.'
                        || ((chars[i] == 'e' || chars[i] == 'E')
                            && i + 1 < chars.len()
                            && (chars[i + 1].is_ascii_digit()
                                || ((chars[i + 1] == '-' || chars[i + 1] == '+')
                                    && next_is_digit(&chars, i + 1))))
                        || (i > start
                            && (chars[i] == '-' || chars[i] == '+')
                            && matches!(chars[i - 1], 'e' | 'E')))
                {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                if text.parse::<f64>().is_err() {
                    return Err(format!("invalid number `{text}` in move_when"));
                }
                tokens.push(Token::Num(text));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(word));
            }
            other => return Err(format!("unsupported character `{other}` in move_when")),
        }
    }
    Ok(tokens)
}

fn next_is_digit(chars: &[char], i: usize) -> bool {
    chars.get(i + 1).is_some_and(char::is_ascii_digit)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn keyword(&mut self, word: &str) -> bool {
        if matches!(self.peek(), Some(Token::Ident(w)) if w.eq_ignore_ascii_case(word)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn literal(&mut self) -> Result<String, String> {
        match self.next() {
            Some(Token::Str(text) | Token::Num(text)) => Ok(text),
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("true") => Ok("true".into()),
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("false") => Ok("false".into()),
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("null") => Err(
                "compare with NULL using IS NULL / IS NOT NULL in move_when".into(),
            ),
            Some(token) => Err(unsupported(&token)),
            None => Err("move_when ends where a constant was expected".into()),
        }
    }

    fn term(&mut self) -> Result<PredicateTerm, String> {
        let column = match self.next() {
            Some(Token::Ident(word)) => {
                if self.peek() == Some(&Token::LParen) {
                    return Err(format!(
                        "function call `{}(...)` is not allowed in move_when",
                        word.to_ascii_lowercase()
                    ));
                }
                if ["not", "or", "and", "select", "exists"]
                    .iter()
                    .any(|kw| word.eq_ignore_ascii_case(kw))
                {
                    return Err(unsupported(&Token::Ident(word)));
                }
                word.to_ascii_lowercase()
            }
            Some(Token::Quoted(name)) => name,
            Some(token) => return Err(unsupported(&token)),
            None => return Err("move_when is empty".into()),
        };
        match self.next() {
            Some(Token::Cmp(op)) => {
                let op = match op {
                    "=" => PredicateOp::Eq,
                    "<>" => PredicateOp::Ne,
                    "<" => PredicateOp::Lt,
                    "<=" => PredicateOp::Le,
                    ">" => PredicateOp::Gt,
                    _ => PredicateOp::Ge,
                };
                Ok(PredicateTerm {
                    column,
                    op,
                    values: vec![self.literal()?],
                })
            }
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("in") => {
                if self.next() != Some(Token::LParen) {
                    return Err("IN in move_when must be followed by a parenthesized list".into());
                }
                let mut values = vec![self.literal()?];
                loop {
                    match self.next() {
                        Some(Token::Comma) => values.push(self.literal()?),
                        Some(Token::RParen) => break,
                        Some(token) => return Err(unsupported(&token)),
                        None => return Err("unterminated IN list in move_when".into()),
                    }
                }
                Ok(PredicateTerm {
                    column,
                    op: PredicateOp::In,
                    values,
                })
            }
            Some(Token::Ident(word)) if word.eq_ignore_ascii_case("is") => {
                let negated = self.keyword("NOT");
                if !self.keyword("NULL") {
                    return Err("IS in move_when must be followed by [NOT] NULL".into());
                }
                Ok(PredicateTerm {
                    column,
                    op: if negated {
                        PredicateOp::IsNotNull
                    } else {
                        PredicateOp::IsNull
                    },
                    values: Vec::new(),
                })
            }
            Some(token) => Err(unsupported(&token)),
            None => Err(format!("move_when term on `{column}` has no operator")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_comparisons_in_and_null_terms() {
        let p = MovePredicate::parse(
            "Status = 'closed' AND amount >= 10.5 and kind IN ('a', 'b') AND ts IS NOT NULL AND z IS NULL",
        )
        .unwrap();
        assert_eq!(p.terms.len(), 5);
        assert_eq!(p.terms[0].column, "status");
        assert_eq!(p.terms[1].op, PredicateOp::Ge);
        assert_eq!(p.terms[1].values, vec!["10.5"]);
        assert_eq!(p.terms[2].values, vec!["a", "b"]);
        assert_eq!(p.terms[3].op, PredicateOp::IsNotNull);
        assert_eq!(p.columns(), vec!["status", "amount", "kind", "ts", "z"]);
    }

    #[test]
    fn compiles_with_escaped_literals_and_quoted_identifiers() {
        let p = MovePredicate::parse("\"Odd\"\"Col\" = 'it''s' AND n < -3").unwrap();
        assert_eq!(
            p.to_sql("hot"),
            "hot.\"Odd\"\"Col\" = 'it''s' AND hot.\"n\" < '-3'"
        );
        let p = MovePredicate::parse("p = 'a\\b'").unwrap();
        assert_eq!(p.to_sql("h"), "h.\"p\" = E'a\\\\b'");
    }

    #[test]
    fn rejects_unsupported_constructs() {
        for bad in [
            "a = 1 OR b = 2",
            "NOT a = 1",
            "created < now()",
            "a = (SELECT 1)",
            "a = 1::int",
            "a + 1 = 2",
            "a = NULL",
            "a = b",
            "",
            "a =",
            "a = 1 AND",
            "a IN ()",
            "a IS 3",
            "a = 'x",
        ] {
            assert!(MovePredicate::parse(bad).is_err(), "should reject: {bad}");
        }
    }

    #[test]
    fn serde_roundtrip_and_validation() {
        let p = MovePredicate::parse("a = 1").unwrap();
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(serde_json::from_value::<MovePredicate>(v).unwrap(), p);
        let bad = MovePredicate {
            terms: vec![PredicateTerm {
                column: "a".into(),
                op: PredicateOp::Eq,
                values: vec![],
            }],
        };
        assert!(bad.validate().is_err());
    }
}
