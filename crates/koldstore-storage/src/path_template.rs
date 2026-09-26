//! Object path templates.

pub use koldstore_common::{
    encode_path_segment, join_object_key, manifest_object_key, normalize_table_prefix,
};

/// Path template using pg-koldstore placeholder names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate {
    template: String,
}

impl PathTemplate {
    /// Creates a template without validation.
    #[must_use]
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
        }
    }

    /// Returns the raw template.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.template
    }

    /// Renders the template for a namespace/table and optional scope.
    ///
    /// # Errors
    ///
    /// Returns an error when `{scopeId}` is required but no scope is supplied.
    pub fn render(
        &self,
        namespace: &str,
        table_name: &str,
        scope_id: Option<&str>,
    ) -> Result<String, String> {
        if self.template.contains("{scopeId}") && scope_id.is_none() {
            return Err("scopeId is required by path template".to_string());
        }
        let rendered = self
            .template
            .replace("{namespace}", &encode_path_segment(namespace))
            .replace("{tableName}", &encode_path_segment(table_name))
            .replace("{scopeId}", scope_id.unwrap_or(""));
        if rendered.contains('{') || rendered.contains('}') {
            return Err("path template contains unresolved placeholders".to_string());
        }
        Ok(rendered)
    }
}

/// Renders and normalizes a regular table prefix.
///
/// # Errors
///
/// Returns the template rendering error when placeholders are invalid.
pub fn render_regular_table_prefix(
    template: &PathTemplate,
    namespace: &str,
    table_name: &str,
) -> Result<String, String> {
    template
        .render(namespace, table_name, None)
        .map(|rendered| normalize_table_prefix(&rendered))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_and_join_default_template() {
        let tmpl = PathTemplate::new("{namespace}/{tableName}/");
        let prefix = render_regular_table_prefix(&tmpl, "app", "items").unwrap();
        assert_eq!(prefix, "app/items/");
        assert_eq!(
            join_object_key(&prefix, "manifest.json"),
            "app/items/manifest.json"
        );
    }

    #[test]
    fn odd_names_cannot_escape_their_prefix() {
        let tmpl = PathTemplate::new("{namespace}/{tableName}/");
        assert_eq!(
            render_regular_table_prefix(&tmpl, "my app", "../../etc/x").unwrap(),
            "my%20app/%2E%2E%2F%2E%2E%2Fetc%2Fx/"
        );
        assert_eq!(render_regular_table_prefix(&tmpl, "app", "Café").unwrap(), "app/Caf%C3%A9/");
        assert_eq!(render_regular_table_prefix(&tmpl, "app", "Mixed_Case-1").unwrap(), "app/Mixed_Case-1/");
    }
}
