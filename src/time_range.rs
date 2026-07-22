//! Extracts `start`/`end` time bounds from filter predicates on the `timestamp`
//! column so DataFusion's time-range filters become Loki's `start`/`end` query
//! params instead of full-corpus scans followed by client-side filtering.

use chrono::{DateTime, Utc};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::schema::COL_TIMESTAMP;

#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

impl TimeRange {
    pub fn unbounded() -> Self {
        Self { start: None, end: None }
    }

    fn tighten_start(&mut self, candidate: DateTime<Utc>) {
        self.start = Some(match self.start {
            Some(existing) => existing.max(candidate),
            None => candidate,
        });
    }

    fn tighten_end(&mut self, candidate: DateTime<Utc>) {
        self.end = Some(match self.end {
            Some(existing) => existing.min(candidate),
            None => candidate,
        });
    }
}

/// Scans a set of filter expressions for bounds on the `timestamp` column and
/// returns the tightest known `[start, end)` range. Expressions that
/// contribute to the range are still considered "exact" pushdowns by the
/// caller and need not be re-applied by DataFusion.
pub fn extract_time_range(filters: &[Expr]) -> TimeRange {
    let mut range = TimeRange::unbounded();
    for filter in filters {
        for conjunct in crate::pushdown::split_conjuncts(filter) {
            apply_conjunct(&conjunct, &mut range);
        }
    }
    range
}

fn apply_conjunct(expr: &Expr, range: &mut TimeRange) {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return;
    };

    let (col_is_left, ts) = match (as_timestamp_column(left), as_timestamp_column(right)) {
        (true, _) => (true, literal_timestamp(right)),
        (_, true) => (false, literal_timestamp(left)),
        _ => return,
    };
    let Some(ts) = ts else { return };

    // Normalize so we always reason in terms of "timestamp OP literal".
    let op = if col_is_left { *op } else { flip(*op) };

    match op {
        Operator::Gt | Operator::GtEq => range.tighten_start(ts),
        Operator::Lt | Operator::LtEq => range.tighten_end(ts),
        Operator::Eq => {
            range.tighten_start(ts);
            range.tighten_end(ts);
        }
        _ => {}
    }
}

fn as_timestamp_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name == COL_TIMESTAMP)
}

fn flip(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

fn literal_timestamp(expr: &Expr) -> Option<DateTime<Utc>> {
    match expr {
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(ns), _)) => {
            DateTime::<Utc>::from_timestamp(
                ns.div_euclid(1_000_000_000),
                (ns.rem_euclid(1_000_000_000)) as u32,
            )
        }
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(us), _)) => {
            DateTime::<Utc>::from_timestamp_micros(*us)
        }
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), _)) => {
            DateTime::<Utc>::from_timestamp_millis(*ms)
        }
        Expr::Literal(ScalarValue::TimestampSecond(Some(s), _)) => DateTime::<Utc>::from_timestamp(*s, 0),
        Expr::Literal(ScalarValue::Utf8(Some(s))) => DateTime::parse_from_rfc3339(s).ok().map(|d| d.with_timezone(&Utc)),
        _ => None,
    }
}

/// Returns true if this conjunct fully constrains a timestamp bound (and thus
/// can be marked exact for pushdown purposes).
pub fn is_timestamp_bound(expr: &Expr) -> bool {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return false;
    };
    let touches_ts = as_timestamp_column(left) || as_timestamp_column(right);
    let has_literal = literal_timestamp(left).is_some() || literal_timestamp(right).is_some();
    touches_ts
        && has_literal
        && matches!(
            op,
            Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;
    use datafusion::prelude::lit;
    use datafusion::scalar::ScalarValue;

    fn ts_lit(rfc3339: &str) -> Expr {
        let dt = DateTime::parse_from_rfc3339(rfc3339).unwrap().with_timezone(&Utc);
        Expr::Literal(ScalarValue::TimestampNanosecond(
            Some(dt.timestamp_nanos_opt().unwrap()),
            None,
        ))
    }

    #[test]
    fn extracts_gt_and_lt_bounds() {
        let filters = vec![
            col(COL_TIMESTAMP).gt(ts_lit("2026-01-01T00:00:00Z")),
            col(COL_TIMESTAMP).lt(ts_lit("2026-01-02T00:00:00Z")),
        ];
        let range = extract_time_range(&filters);
        assert!(range.start.is_some());
        assert!(range.end.is_some());
        assert!(range.start.unwrap() < range.end.unwrap());
    }

    #[test]
    fn ignores_unrelated_filters() {
        let filters = vec![col("job").eq(lit("x"))];
        let range = extract_time_range(&filters);
        assert!(range.start.is_none() && range.end.is_none());
    }
}
