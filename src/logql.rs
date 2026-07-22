//! Assembles a final LogQL query string from a base stream selector plus
//! pushed-down label matchers and line filters.

/// Merges pushed-down label matchers into the base selector's brace group and
/// appends any line filters after it.
///
/// `base_selector` is expected to look like `{job="myapp"}` (braces included).
/// If it's just a bare label set without braces, braces are added.
pub fn build_logql(base_selector: &str, extra_matchers: &[String], line_filters: &[String]) -> String {
    let trimmed = base_selector.trim();
    let (open, inner, close) = split_selector(trimmed);

    let mut all_matchers: Vec<&str> = Vec::new();
    if !inner.trim().is_empty() {
        all_matchers.push(inner.trim());
    }
    for m in extra_matchers {
        all_matchers.push(m);
    }

    let mut query = format!("{open}{}{close}", all_matchers.join(", "));
    for filter in line_filters {
        query.push(' ');
        query.push_str(filter);
    }
    query
}

fn split_selector(selector: &str) -> (&'static str, &str, &'static str) {
    if let Some(inner) = selector.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        ("{", inner, "}")
    } else {
        ("{", selector, "}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_matchers_into_braces() {
        let q = build_logql(r#"{job="myapp"}"#, &[r#"env="prod""#.to_string()], &[]);
        assert_eq!(q, r#"{job="myapp", env="prod"}"#);
    }

    #[test]
    fn appends_line_filters() {
        let q = build_logql(r#"{job="myapp"}"#, &[], &[r#"|= "panic""#.to_string()]);
        assert_eq!(q, r#"{job="myapp"} |= "panic""#);
    }

    #[test]
    fn handles_bare_selector_without_braces() {
        let q = build_logql(r#"job="myapp""#, &[], &[]);
        assert_eq!(q, r#"{job="myapp"}"#);
    }

    #[test]
    fn handles_empty_base_selector() {
        let q = build_logql("{}", &[r#"job="myapp""#.to_string()], &[]);
        assert_eq!(q, r#"{job="myapp"}"#);
    }
}
