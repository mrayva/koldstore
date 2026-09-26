//! Type-safe PostgreSQL table names.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{ident, KoldstoreError, Result};

/// A validated one- or two-part PostgreSQL table name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableName(String);

impl TableName {
    /// Parses a one- or two-part PostgreSQL table name, as typed or as
    /// `regclass::text` prints it: a part may be double-quoted (`"MixedCase"`,
    /// `"select"`) and is stored without the quotes. The part's content must
    /// still be a plain identifier (ASCII letters, digits, `_`).
    ///
    /// # Errors
    ///
    /// Returns an error when the table name is blank, multipart beyond
    /// `schema.table`, or a part contains unsafe identifier characters.
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref().trim();
        let valid = split_name_parts(value).filter(|parts| {
            matches!(parts.len(), 1 | 2) && parts.iter().all(|part| ident::is_safe_identifier(part))
        });

        match valid {
            Some(parts) => Ok(Self(parts.join("."))),
            None => Err(KoldstoreError::InvalidIdentifier {
                kind: "table name",
                value: value.to_string(),
            }),
        }
    }

    /// Returns the normalized table name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the optional schema component.
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.0.split_once('.').map(|(schema, _)| schema)
    }

    /// Returns the relation component.
    #[must_use]
    pub fn relation(&self) -> &str {
        self.0
            .split_once('.')
            .map_or(self.0.as_str(), |(_, relation)| relation)
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        match self.schema() {
            Some(schema) => format!(
                "{}.{}",
                ident::quote_ident(schema),
                ident::quote_ident(self.relation())
            ),
            None => ident::quote_ident(self.relation()),
        }
    }
}

/// Splits `schema.name` text into its parts, honouring double-quoted parts
/// (with `""` as an escaped quote) so a dot inside quotes does not split.
fn split_name_parts(value: &str) -> Option<Vec<String>> {
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

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TableName {
    type Err = KoldstoreError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

/// Parsed table name with separately addressable schema and relation parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedTableName {
    /// Optional schema name.
    pub schema: Option<String>,
    /// Relation name.
    pub name: String,
}

impl QualifiedTableName {
    /// Parses an unquoted one- or two-part PostgreSQL identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for blank, multipart, or unsafe identifier text.
    pub fn parse(value: &str) -> Result<Self> {
        let table = TableName::parse(value)?;
        Ok(Self::from_table_name(&table))
    }

    /// Returns the normalized [`TableName`] for this relation.
    ///
    /// # Errors
    ///
    /// Returns an error when schema/name components are no longer valid.
    pub fn as_table_name(&self) -> Result<TableName> {
        TableName::parse(self.display_name())
    }

    /// Builds a [`QualifiedTableName`] from a validated [`TableName`].
    #[must_use]
    pub fn from_table_name(table: &TableName) -> Self {
        Self {
            schema: table.schema().map(str::to_string),
            name: table.relation().to_string(),
        }
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        self.as_table_name()
            .map(|table| table.quoted())
            .unwrap_or_else(|_| match &self.schema {
                Some(schema) => format!("\"{schema}\".\"{}\"", self.name),
                None => format!("\"{}\"", self.name),
            })
    }

    fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.{}", self.name),
            None => self.name.clone(),
        }
    }
}
