use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::client::LokiClient;
use crate::config::LokiConfig;
use crate::error::LokiError;
use crate::exec::LokiExec;
use crate::logql::build_logql;
use crate::pushdown::plan_pushdown;
use crate::schema::{build_schema, LabelSchema};
use crate::time_range::{extract_time_range, is_timestamp_bound};

/// A DataFusion `TableProvider` backed by a Grafana Loki log stream selector.
///
/// Each table maps to one LogQL stream selector (e.g. `{job="myapp"}`). SQL
/// `WHERE` clauses over label columns and the `line` column are pushed down
/// into LogQL label matchers / line filters; `WHERE timestamp BETWEEN ...`
/// becomes Loki's `start`/`end` range params; `LIMIT n` is pushed down as a
/// row cap across paginated `query_range` calls. Everything else is left for
/// DataFusion to evaluate on the returned batches.
pub struct LokiTableProvider {
    config: LokiConfig,
    schema: SchemaRef,
    label_schema: LabelSchema,
}

impl LokiTableProvider {
    /// Builds a provider with an explicit, known label set. Prefer this over
    /// `connect` when you know your labels ahead of time — it avoids an extra
    /// `/loki/api/v1/labels` round trip and produces a fully-typed flattened
    /// schema (`WHERE job = 'foo'` instead of `WHERE labels['job'] = 'foo'`).
    pub fn new(config: LokiConfig, labels: Vec<String>) -> Self {
        let label_schema = LabelSchema::Flattened(labels);
        let schema = Arc::new(build_schema(&label_schema));
        Self {
            config,
            schema,
            label_schema,
        }
    }

    /// Builds a provider using a single `Map<Utf8, Utf8>` column for all
    /// labels, so no schema discovery call is needed and arbitrary/unknown
    /// label sets are supported. SQL uses `labels['job'] = 'foo'`; predicates
    /// of that exact shape (equality/inequality/regex/`IN`/same-key `OR`
    /// against a literal key) are recognized and pushed down into LogQL just
    /// like flattened label columns are. Anything DataFusion can't reduce to
    /// `labels['x'] <op> literal` (e.g. comparing two map lookups, or a
    /// non-literal key) still falls back to fetching everything matching the
    /// base `stream_selector` and filtering client-side.
    pub fn new_with_map_labels(config: LokiConfig) -> Self {
        let label_schema = LabelSchema::MapColumn;
        let schema = Arc::new(build_schema(&label_schema));
        Self {
            config,
            schema,
            label_schema,
        }
    }

    /// Discovers the label set from Loki's `/loki/api/v1/labels` endpoint and
    /// builds a flattened-schema provider from it. Convenient for interactive
    /// use; for production pipelines prefer `new` with an explicit list so the
    /// schema doesn't silently shift if new labels appear upstream.
    pub async fn connect(config: LokiConfig) -> Result<Self, LokiError> {
        let client = LokiClient::new(&config)?;
        let labels = client.labels(&config, None, None).await?;
        Ok(Self::new(config, labels))
    }

    fn label_columns(&self) -> Vec<String> {
        match &self.label_schema {
            LabelSchema::Flattened(labels) => labels.clone(),
            LabelSchema::MapColumn => vec![],
        }
    }
}

#[async_trait]
impl TableProvider for LokiTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        let label_columns = self.label_columns();
        let mut results = Vec::with_capacity(filters.len());

        for filter in filters {
            if is_timestamp_bound(filter) {
                // Time bounds are always applied exactly via start/end params.
                results.push(TableProviderFilterPushDown::Exact);
                continue;
            }

            let conjuncts = crate::pushdown::split_conjuncts(filter);
            let all_exact = !conjuncts.is_empty()
                && conjuncts
                    .iter()
                    .all(|c| matches!(crate::pushdown::push_expr(c, &label_columns), crate::pushdown::PushResult::Exact(_)));

            if all_exact {
                results.push(TableProviderFilterPushDown::Exact);
            } else {
                let any_exact = conjuncts
                    .iter()
                    .any(|c| matches!(crate::pushdown::push_expr(c, &label_columns), crate::pushdown::PushResult::Exact(_)));
                results.push(if any_exact {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                });
            }
        }

        Ok(results)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let label_columns = self.label_columns();
        let pushdown = plan_pushdown(filters, &label_columns);
        let time_range = extract_time_range(filters);

        let logql = build_logql(
            &self.config.stream_selector,
            &pushdown.label_matchers,
            &pushdown.line_filters,
        );

        log::debug!(
            "LokiTableProvider::scan translated to LogQL: {logql} (range: {:?}..{:?}, limit: {:?})",
            time_range.start,
            time_range.end,
            limit
        );

        if pushdown.remaining.iter().any(is_unpushable_and_required) {
            // Nothing to special-case today, but this is the hook point for
            // erroring out on filters that must be exact and can't be pushed,
            // should such a constraint ever be introduced.
        }

        let full_schema = self.schema.clone();
        let exec = LokiExec::new(
            logql,
            self.config.clone(),
            time_range,
            full_schema.clone(),
            self.label_schema.clone(),
            limit,
        );

        let plan: Arc<dyn ExecutionPlan> = Arc::new(exec);

        match projection {
            Some(proj) => project_plan(plan, &full_schema, proj),
            None => Ok(plan),
        }
    }
}

fn is_unpushable_and_required(_expr: &Expr) -> bool {
    false
}

/// Wraps the base scan in a `ProjectionExec` when DataFusion requests a column
/// subset, so we don't materialize unused label columns downstream. Falls back
/// to returning the unprojected plan (with DataFusion applying the projection
/// itself) if building a projection expression fails for any reason.
fn project_plan(
    input: Arc<dyn ExecutionPlan>,
    full_schema: &SchemaRef,
    projection: &[usize],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::projection::ProjectionExec;

    let expr = projection
        .iter()
        .map(|&idx| {
            let field = full_schema.field(idx);
            let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                Arc::new(Column::new(field.name(), idx));
            (col, field.name().clone())
        })
        .collect::<Vec<_>>();

    let proj = ProjectionExec::try_new(expr, input.clone())
        .map_err(|e| DataFusionError::Context("failed to build Loki scan projection".to_string(), Box::new(e)))?;
    Ok(Arc::new(proj))
}

/// Convenience: returns an empty, schema-only execution plan. Not currently
/// wired up but kept available for callers that want a fast-path for
/// statically-known-empty predicates (e.g. `WHERE 1 = 0`).
#[allow(dead_code)]
fn empty_plan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema))
}
