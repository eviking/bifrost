use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// Column name for the log entry's nanosecond timestamp.
pub const COL_TIMESTAMP: &str = "timestamp";
/// Column name for the raw log line.
pub const COL_LINE: &str = "line";
/// Column name for the struct-of-labels column (used when `flatten_labels` is false).
pub const COL_LABELS: &str = "labels";

/// Controls how Loki stream labels are projected into the Arrow schema.
#[derive(Debug, Clone)]
pub enum LabelSchema {
    /// Each known label becomes its own top-level `Utf8` column, e.g. `job`, `env`,
    /// `pod`. Labels present on a stream but not listed here are dropped. This is
    /// the most SQL-ergonomic mode: `WHERE job = 'foo'` works directly and predicate
    /// pushdown can translate it straight into a LogQL selector.
    Flattened(Vec<String>),

    /// All labels are placed into a single `labels` column of type
    /// `Map<Utf8, Utf8>`. Works for arbitrary/unknown label sets but requires
    /// `labels['job'] = 'foo'` style SQL and labels can't be pushed down as
    /// LogQL selectors automatically (only manual selectors in the base
    /// `stream_selector` apply).
    MapColumn,
}

/// Builds the full Arrow schema for a Loki-backed table given a label mode.
///
/// Resulting column order is always: `timestamp`, `line`, then label column(s).
pub fn build_schema(label_schema: &LabelSchema) -> Schema {
    let mut fields = vec![
        Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new(COL_LINE, DataType::Utf8, false),
    ];

    match label_schema {
        LabelSchema::Flattened(labels) => {
            for label in labels {
                fields.push(Field::new(label, DataType::Utf8, true));
            }
        }
        LabelSchema::MapColumn => {
            fields.push(map_of_strings_field(COL_LABELS));
        }
    }

    Schema::new(fields)
}

/// Constructs a `Map<Utf8, Utf8>` field, matching the layout `arrow::array::MapBuilder`
/// produces by default (entries struct named "entries" with "keys"/"values" children,
/// sorted keys = false) — this must stay in sync with `MapBuilder`'s defaults in
/// `convert.rs`, so it's built by finishing an empty builder rather than by hand.
fn map_of_strings_field(name: &str) -> Field {
    use arrow::array::{Array, MapBuilder, StringBuilder};

    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let map_array = builder.finish();
    Field::new(name, map_array.data_type().clone(), true)
}
