//! Type-safe PostgreSQL table names.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{ident, KoldstoreError, Result};

/// A validated one- or two-part PostgreSQL table name.
///
/// Parts may be any valid identifier (spaces, quotes, dots, non-ASCII). The text
/// form ([`Self::as_str`]) prints a part bare when it is a plain identifier and
/// double-quoted otherwise, exactly like `regclass::text`, and parses back to
/// the same name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableName {
    text: String,
    schema: Option<String>,
    name: String,
}

impl Serialize for TableName {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text)
    }
}

impl<'de> Deserialize<'de> for TableName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

impl TableName {
    /// Parses a one- or two-part PostgreSQL table name, as typed or as
    /// `regclass::text` prints it. A part may be double-quoted (`"Odd Name"`,
    /// `"a""b"`) and is stored without the quotes; an unquoted part must be a
    /// plain identifier and is taken as typed (no case folding).
    ///
    /// # Errors
    ///
    /// Returns an error when the name is blank, has more than two parts, an
    /// unquoted part is not a plain identifier, or a part is not a valid
    /// identifier.
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        let value = value.as_ref().trim();
        let invalid = || KoldstoreError::InvalidIdentifier {
            kind: "table name",
            value: value.to_string(),
        };
        let parts = ident::split_qualified(value).ok_or_else(invalid)?;
        if !matches!(parts.len(), 1 | 2) || !parts.iter().all(|part| ident::is_valid_identifier(part)) {
            return Err(invalid());
        }
        // A bare part must be a plain identifier: `a b` or `1x` need quotes.
        if !bare_parts_are_plain(value) {
            return Err(invalid());
        }
        let mut parts = parts.into_iter();
        let first = parts.next().ok_or_else(invalid)?;
        Ok(match parts.next() {
            Some(name) => Self::from_parts(Some(first), name),
            None => Self::from_parts(None, first),
        })
    }

    /// Builds a table name from already-separated parts (as found in the
    /// catalog), which may contain any valid identifier.
    ///
    /// # Errors
    ///
    /// Returns an error when a part is not a valid identifier.
    pub fn new(schema: Option<&str>, name: &str) -> Result<Self> {
        let valid = ident::is_valid_identifier(name) && schema.is_none_or(ident::is_valid_identifier);
        if !valid {
            return Err(KoldstoreError::InvalidIdentifier {
                kind: "table name",
                value: schema.map_or_else(|| name.to_string(), |schema| format!("{schema}.{name}")),
            });
        }
        Ok(Self::from_parts(schema.map(str::to_string), name.to_string()))
    }

    fn from_parts(schema: Option<String>, name: String) -> Self {
        let text = match &schema {
            Some(schema) => format!("{}.{}", ident::display_ident(schema), ident::display_ident(&name)),
            None => ident::display_ident(&name),
        };
        Self { text, schema, name }
    }

    /// Returns the normalized text form (`regclass::text` style).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Returns the optional schema component (unquoted content).
    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns the relation component (unquoted content).
    #[must_use]
    pub fn relation(&self) -> &str {
        &self.name
    }

    /// Returns a safely quoted SQL relation reference.
    #[must_use]
    pub fn quoted(&self) -> String {
        match self.schema() {
            Some(schema) => format!("{}.{}", ident::quote_ident(schema), ident::quote_ident(self.relation())),
            None => ident::quote_ident(self.relation()),
        }
    }
}

/// True when every part of `value` that is not double-quoted is a plain
/// identifier.
fn bare_parts_are_plain(value: &str) -> bool {
    let mut rest = value;
    loop {
        if let Some(quoted) = rest.strip_prefix('"') {
            // skip to the closing quote, honouring doubled quotes
            let bytes = quoted.as_bytes();
            let mut index = 0;
            loop {
                match bytes.get(index) {
                    Some(b'"') if bytes.get(index + 1) == Some(&b'"') => index += 2,
                    Some(b'"') => break,
                    Some(_) => index += 1,
                    None => return false,
                }
            }
            rest = &quoted[index + 1..];
        } else {
            let end = rest.find('.').unwrap_or(rest.len());
            if !ident::is_safe_identifier(&rest[..end]) {
                return false;
            }
            rest = &rest[end..];
        }
        match rest.strip_prefix('.') {
            Some(next) => rest = next,
            None => return rest.is_empty(),
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
    /// Parses a one- or two-part PostgreSQL identifier (see [`TableName::parse`]).
    ///
    /// # Errors
    ///
    /// Returns an error for blank, multipart, or malformed identifier text.
    pub fn parse(value: &str) -> Result<Self> {
        let table = TableName::parse(value)?;
        Ok(Self::from_table_name(&table))
    }

    /// Builds a name from already-separated catalog parts.
    ///
    /// # Errors
    ///
    /// Returns an error when a part is not a valid identifier.
    pub fn new(schema: Option<&str>, name: &str) -> Result<Self> {
        Ok(Self::from_table_name(&TableName::new(schema, name)?))
    }

    /// Returns the normalized [`TableName`] for this relation.
    ///
    /// # Errors
    ///
    /// Returns an error when schema/name components are no longer valid.
    pub fn as_table_name(&self) -> Result<TableName> {
        TableName::new(self.schema.as_deref(), &self.name)
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
        match &self.schema {
            Some(schema) => format!("{}.{}", ident::quote_ident(schema), ident::quote_ident(&self.name)),
            None => ident::quote_ident(&self.name),
        }
    }

    /// The `regclass::text`-style text form.
    #[must_use]
    pub fn display_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", ident::display_ident(schema), ident::display_ident(&self.name)),
            None => ident::display_ident(&self.name),
        }
    }
}
