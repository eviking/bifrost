//! Translates DataFusion filter expressions into LogQL fragments where possible.
//!
//! Loki's LogQL stream selector (`{label="value", ...}`) only supports equality,
//! inequality, and regex matches against labels, plus optional line filters
//! (`|= "substr"`, `!= "substr"`, `|~ "regex"`, `!~ "regex"`) against the raw
//! log line. `IN` / `NOT IN` lists and same-column `OR` chains are translated
//! into LogQL's regex-alternation form (`col=~"a|b|c"`), since that's the
//! closest LogQL equivalent to "one of these values". `labels['x'] = 'y'`
//! (map-column mode) is recognized as a matcher on label `x`. Anything else
//! (numeric comparisons on the line, OR trees mixing different columns,
//! arbitrary functions, etc.) cannot be pushed down and is left for
//! DataFusion to evaluate after the scan, via `TableProviderFilterPushDown::Inexact`.

use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::schema::COL_LINE;

/// Names DataFusion registers `labels['x']`-style indexing scalar functions
/// under. SQL parsed against a schema where `labels` is actually `Map`-typed
/// resolves to `get_field` (`datafusion_functions::core::getfield::GetFieldFunc`);
/// the `IndexAccessor::index()` builder API instead always produces
/// `array_element` regardless of the underlying column's real type, since it
/// has no schema awareness at expression-build time. Both are recognized so
/// pushdown works regardless of which path an `Expr` tree came from.
const MAP_INDEX_FNS: &[&str] = &["get_field", "array_element"];

/// The outcome of attempting to push a single `Expr` into LogQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushResult {
    /// Fully translated; DataFusion does not need to re-check this predicate.
    Exact(String),
    /// Not translatable; DataFusion must still apply this predicate itself.
    Unsupported,
}

/// What a predicate is being evaluated against: the raw log line, a flattened
/// label column, or (in `LabelSchema::MapColumn` mode) a specific key inside
/// the `labels` map column via `labels['x']`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Target {
    Line,
    Label(String),
}

/// Resolves an `Expr` to a pushable target, or `None` if it isn't one we
/// recognize (an arbitrary column, a nested expression, etc.).
///
/// `label_columns` is the flattened label column list; in map-column mode
/// it's empty and only `labels['x']` map-access expressions resolve.
fn resolve_target(expr: &Expr, label_columns: &[String]) -> Option<Target> {
    match expr {
        Expr::Column(c) if c.name == COL_LINE => Some(Target::Line),
        Expr::Column(c) if label_columns.iter().any(|l| l == &c.name) => {
            Some(Target::Label(c.name.clone()))
        }
        Expr::ScalarFunction(f) if MAP_INDEX_FNS.contains(&f.func.name()) && f.args.len() == 2 => {
            let is_labels_col = matches!(&f.args[0], Expr::Column(c) if c.name == crate::schema::COL_LABELS);
            if !is_labels_col {
                return None;
            }
            literal_str(&f.args[1]).map(Target::Label)
        }
        _ => None,
    }
}

fn target_to_fragment(target: &Target, kind: MatchKind, value: &str) -> String {
    match target {
        Target::Line => format!("{} {}", kind.line_op(), quote(value)),
        Target::Label(name) => format!("{name}{}{}", kind.label_op(), quote(value)),
    }
}

#[derive(Debug, Clone, Copy)]
enum MatchKind {
    Eq,
    NotEq,
    Regex,
    NotRegex,
}

impl MatchKind {
    fn line_op(self) -> &'static str {
        match self {
            MatchKind::Eq => "|=",
            MatchKind::NotEq => "!=",
            MatchKind::Regex => "|~",
            MatchKind::NotRegex => "!~",
        }
    }

    fn label_op(self) -> &'static str {
        match self {
            MatchKind::Eq => "=",
            MatchKind::NotEq => "!=",
            MatchKind::Regex => "=~",
            MatchKind::NotRegex => "!~",
        }
    }
}

/// Attempts to translate a single filter expression into a LogQL selector
/// fragment (label matcher) or line-filter fragment.
///
/// Label matchers are returned as e.g. `job="foo"` (no braces — caller merges
/// into the `{...}` selector). Line filters are returned as e.g. `|= "foo"`.
pub fn push_expr(expr: &Expr, label_columns: &[String]) -> PushResult {
    match expr {
        Expr::BinaryExpr(be) if be.op == Operator::Or => push_same_target_or(expr, label_columns)
            .unwrap_or(PushResult::Unsupported),
        Expr::BinaryExpr(be) => push_binary(be, label_columns),
        Expr::Like(like) if !like.negated => push_like(like, label_columns, false),
        Expr::Like(like) if like.negated => push_like(like, label_columns, true),
        Expr::InList(in_list) => push_in_list(in_list, label_columns),
        Expr::Not(inner) => match push_expr(inner, label_columns) {
            PushResult::Exact(_) => PushResult::Unsupported, // negation composition kept conservative
            PushResult::Unsupported => PushResult::Unsupported,
        },
        _ => PushResult::Unsupported,
    }
}

fn push_binary(be: &datafusion::logical_expr::BinaryExpr, label_columns: &[String]) -> PushResult {
    use Operator::*;

    // Only handle simple `column OP literal` (or reversed) shapes; conjunctions
    // are handled by the caller splitting on AND before calling push_expr.
    if be.op == And {
        // Shouldn't normally reach here since callers split conjuncts first,
        // but handle gracefully: both sides must be exact.
        return match (push_expr(&be.left, label_columns), push_expr(&be.right, label_columns)) {
            (PushResult::Exact(l), PushResult::Exact(r)) => PushResult::Exact(format!("{l}, {r}")),
            _ => PushResult::Unsupported,
        };
    }

    let (target, op, lit) = match (
        resolve_target(&be.left, label_columns),
        resolve_target(&be.right, label_columns),
    ) {
        (Some(target), None) => (target, be.op, literal_str(&be.right)),
        (None, Some(target)) => (target, flip_operator(be.op), literal_str(&be.left)),
        _ => return PushResult::Unsupported,
    };

    let Some(lit) = lit else {
        return PushResult::Unsupported;
    };

    let kind = match op {
        Eq => MatchKind::Eq,
        NotEq => MatchKind::NotEq,
        RegexMatch | RegexIMatch => MatchKind::Regex,
        RegexNotMatch | RegexNotIMatch => MatchKind::NotRegex,
        _ => return PushResult::Unsupported,
    };

    PushResult::Exact(target_to_fragment(&target, kind, &lit))
}

/// Translates `col IN ('a', 'b', 'c')` / `col NOT IN (...)` into LogQL's
/// regex-alternation form, e.g. `col=~"a|b|c"` / `col!~"a|b|c"`. Only
/// applies when every list element is a string literal (no subqueries, no
/// mixed types) and the target resolves to the line or a label.
fn push_in_list(in_list: &datafusion::logical_expr::expr::InList, label_columns: &[String]) -> PushResult {
    let Some(target) = resolve_target(&in_list.expr, label_columns) else {
        return PushResult::Unsupported;
    };
    let mut values = Vec::with_capacity(in_list.list.len());
    for item in &in_list.list {
        let Some(v) = literal_str(item) else {
            return PushResult::Unsupported;
        };
        values.push(v);
    }
    if values.is_empty() {
        return PushResult::Unsupported;
    }
    let kind = if in_list.negated { MatchKind::NotRegex } else { MatchKind::Regex };
    let alternation = values.iter().map(|v| regex_escape(v)).collect::<Vec<_>>().join("|");
    PushResult::Exact(target_to_fragment(&target, kind, &alternation))
}

/// Translates `col = 'a' OR col = 'b' OR col = 'c'` (same target, all
/// equality, all literals) into LogQL regex alternation. Returns `None`
/// (not `Unsupported`) for shapes this doesn't recognize, so the caller can
/// fall through to the generic `Unsupported` path without duplicating that
/// arm.
fn push_same_target_or(expr: &Expr, label_columns: &[String]) -> Option<PushResult> {
    let mut branches = Vec::new();
    if !collect_or_branches(expr, &mut branches) {
        return None;
    }

    let mut target: Option<Target> = None;
    let mut values = Vec::with_capacity(branches.len());
    for branch in &branches {
        let Expr::BinaryExpr(be) = branch else { return None };
        if be.op != Operator::Eq {
            return None;
        }
        let (this_target, lit) = match (
            resolve_target(&be.left, label_columns),
            resolve_target(&be.right, label_columns),
        ) {
            (Some(t), None) => (t, literal_str(&be.right)),
            (None, Some(t)) => (t, literal_str(&be.left)),
            _ => return None,
        };
        let Some(lit) = lit else { return None };

        match &target {
            Some(t) if *t != this_target => return None,
            _ => target = Some(this_target),
        }
        values.push(lit);
    }

    let target = target?;
    let alternation = values.iter().map(|v| regex_escape(v)).collect::<Vec<_>>().join("|");
    Some(PushResult::Exact(target_to_fragment(&target, MatchKind::Regex, &alternation)))
}

fn collect_or_branches<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) -> bool {
    match expr {
        Expr::BinaryExpr(be) if be.op == Operator::Or => {
            collect_or_branches(&be.left, out) && collect_or_branches(&be.right, out)
        }
        Expr::BinaryExpr(_) => {
            out.push(expr);
            true
        }
        _ => false,
    }
}

/// Escapes regex metacharacters so a literal value used in an `IN`/`OR`
/// alternation matches literally rather than being interpreted as regex
/// syntax by Loki.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn push_like(
    like: &datafusion::logical_expr::Like,
    label_columns: &[String],
    negated: bool,
) -> PushResult {
    let Some(target) = resolve_target(&like.expr, label_columns) else {
        return PushResult::Unsupported;
    };
    let Some(pattern) = literal_str(&like.pattern) else {
        return PushResult::Unsupported;
    };
    // Only translate simple `LIKE '%substr%'` into a line contains-filter;
    // anything with single-char wildcards (`_`) or anchored patterns is left
    // for DataFusion to evaluate exactly.
    if !pattern.starts_with('%') || !pattern.ends_with('%') || pattern.len() < 2 {
        return PushResult::Unsupported;
    }
    let substr = &pattern[1..pattern.len() - 1];
    if substr.contains('%') || substr.contains('_') {
        return PushResult::Unsupported;
    }

    let kind = if negated { MatchKind::NotEq } else { MatchKind::Eq };
    PushResult::Exact(target_to_fragment(&target, kind, substr))
}

fn literal_str(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Some(s.clone()),
        Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

fn flip_operator(op: Operator) -> Operator {
    use Operator::*;
    match op {
        Eq => Eq,
        NotEq => NotEq,
        _ => op,
    }
}

/// Escapes and wraps a string as a Go-style double-quoted LogQL literal.
fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Splits a top-level AND expression into its conjuncts. `WHERE a AND b AND c`
/// is decomposed so each conjunct can be pushed down independently, maximizing
/// how much of the predicate reaches Loki even if some conjuncts aren't
/// translatable.
pub fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(be) if be.op == Operator::And => {
            let mut out = split_conjuncts(&be.left);
            out.extend(split_conjuncts(&be.right));
            out
        }
        other => vec![other.clone()],
    }
}

/// Given a full filter list, partitions into (label selector fragments, line
/// filter fragments, leftover exprs DataFusion must still apply).
pub struct Pushdown {
    pub label_matchers: Vec<String>,
    pub line_filters: Vec<String>,
    pub remaining: Vec<Expr>,
}

pub fn plan_pushdown(filters: &[Expr], label_columns: &[String]) -> Pushdown {
    let mut label_matchers = Vec::new();
    let mut line_filters = Vec::new();
    let mut remaining = Vec::new();

    for filter in filters {
        for conjunct in split_conjuncts(filter) {
            match push_expr(&conjunct, label_columns) {
                PushResult::Exact(fragment) => {
                    if fragment.starts_with("|=")
                        || fragment.starts_with("!=")
                        || fragment.starts_with("|~")
                        || fragment.starts_with("!~")
                    {
                        line_filters.push(fragment);
                    } else {
                        label_matchers.push(fragment);
                    }
                }
                PushResult::Unsupported => remaining.push(conjunct),
            }
        }
    }

    Pushdown {
        label_matchers,
        line_filters,
        remaining,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;
    use datafusion::prelude::lit;

    #[test]
    fn pushes_label_equality() {
        let expr = col("job").eq(lit("myapp"));
        let labels = vec!["job".to_string()];
        assert_eq!(push_expr(&expr, &labels), PushResult::Exact(r#"job="myapp""#.to_string()));
    }

    #[test]
    fn pushes_line_contains() {
        let expr = col(COL_LINE).eq(lit("error"));
        assert_eq!(push_expr(&expr, &[]), PushResult::Exact(r#"|= "error""#.to_string()));
    }

    #[test]
    fn rejects_unknown_column() {
        let expr = col("not_a_label").eq(lit("x"));
        assert_eq!(push_expr(&expr, &[]), PushResult::Unsupported);
    }

    #[test]
    fn splits_and_pushes_conjuncts() {
        let expr = col("job").eq(lit("myapp")).and(col(COL_LINE).eq(lit("panic")));
        let labels = vec!["job".to_string()];
        let pd = plan_pushdown(std::slice::from_ref(&expr), &labels);
        assert_eq!(pd.label_matchers, vec![r#"job="myapp""#.to_string()]);
        assert_eq!(pd.line_filters, vec![r#"|= "panic""#.to_string()]);
        assert!(pd.remaining.is_empty());
    }

    #[test]
    fn pushes_in_list_on_label() {
        let expr = col("level").in_list(vec![lit("error"), lit("warn")], false);
        let labels = vec!["level".to_string()];
        assert_eq!(
            push_expr(&expr, &labels),
            PushResult::Exact(r#"level=~"error|warn""#.to_string())
        );
    }

    #[test]
    fn pushes_not_in_list_on_label() {
        let expr = col("level").in_list(vec![lit("error"), lit("warn")], true);
        let labels = vec!["level".to_string()];
        assert_eq!(
            push_expr(&expr, &labels),
            PushResult::Exact(r#"level!~"error|warn""#.to_string())
        );
    }

    #[test]
    fn pushes_in_list_on_line() {
        let expr = col(COL_LINE).in_list(vec![lit("panic"), lit("fatal")], false);
        assert_eq!(
            push_expr(&expr, &[]),
            PushResult::Exact(r#"|~ "panic|fatal""#.to_string())
        );
    }

    #[test]
    fn rejects_in_list_with_non_literal() {
        let expr = col("level").in_list(vec![lit("error"), col("other")], false);
        let labels = vec!["level".to_string()];
        assert_eq!(push_expr(&expr, &labels), PushResult::Unsupported);
    }

    #[test]
    fn pushes_same_column_or_as_regex_alternation() {
        let expr = col("level").eq(lit("error")).or(col("level").eq(lit("warn")));
        let labels = vec!["level".to_string()];
        assert_eq!(
            push_expr(&expr, &labels),
            PushResult::Exact(r#"level=~"error|warn""#.to_string())
        );
    }

    #[test]
    fn pushes_three_way_or_chain() {
        let expr = col("level")
            .eq(lit("error"))
            .or(col("level").eq(lit("warn")))
            .or(col("level").eq(lit("info")));
        let labels = vec!["level".to_string()];
        assert_eq!(
            push_expr(&expr, &labels),
            PushResult::Exact(r#"level=~"error|warn|info""#.to_string())
        );
    }

    #[test]
    fn rejects_or_across_different_columns() {
        let expr = col("level").eq(lit("error")).or(col("job").eq(lit("myapp")));
        let labels = vec!["level".to_string(), "job".to_string()];
        assert_eq!(push_expr(&expr, &labels), PushResult::Unsupported);
    }

    #[test]
    fn rejects_or_with_non_eq_branch() {
        let expr = col("level").eq(lit("error")).or(col("level").not_eq(lit("warn")));
        let labels = vec!["level".to_string()];
        assert_eq!(push_expr(&expr, &labels), PushResult::Unsupported);
    }

    #[test]
    fn regex_alternation_escapes_metacharacters() {
        let expr = col("level").in_list(vec![lit("a.b"), lit("c|d")], false);
        let labels = vec!["level".to_string()];
        // regex_escape() inserts one backslash before each metachar (producing
        // the regex `a\.b|c\|d`); quote() then escapes that backslash again so
        // the LogQL *string literal* decodes back to that exact regex text —
        // i.e. the query text itself contains a doubled backslash before both
        // the escaped `.` and the escaped `|`.
        assert_eq!(
            push_expr(&expr, &labels),
            PushResult::Exact("level=~\"a\\\\.b|c\\\\|d\"".to_string())
        );
    }

    #[test]
    fn pushes_map_label_equality() {
        use datafusion::functions_nested::expr_ext::IndexAccessor;

        let expr = col(crate::schema::COL_LABELS).index(lit("level")).eq(lit("error"));
        // Map mode: label_columns is empty, only get_field(labels, 'x') resolves.
        assert_eq!(push_expr(&expr, &[]), PushResult::Exact(r#"level="error""#.to_string()));
    }

    #[test]
    fn pushes_map_label_in_list() {
        use datafusion::functions_nested::expr_ext::IndexAccessor;

        let expr = col(crate::schema::COL_LABELS)
            .index(lit("level"))
            .in_list(vec![lit("error"), lit("warn")], false);
        assert_eq!(
            push_expr(&expr, &[]),
            PushResult::Exact(r#"level=~"error|warn""#.to_string())
        );
    }

    #[test]
    fn rejects_map_access_on_non_labels_column() {
        use datafusion::functions_nested::expr_ext::IndexAccessor;

        let expr = col("other_map").index(lit("level")).eq(lit("error"));
        assert_eq!(push_expr(&expr, &[]), PushResult::Unsupported);
    }
}
