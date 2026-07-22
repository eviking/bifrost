use std::sync::Arc;

use arrow::array::{
    ArrayRef, MapBuilder, RecordBatch, StringArray, StringBuilder, TimestampNanosecondArray,
};
use arrow_schema::SchemaRef;

use crate::client::LogEntry;
use crate::error::Result;
use crate::schema::LabelSchema;

/// Converts a batch of decoded Loki log entries into a single Arrow `RecordBatch`
/// matching `schema`. Entries are assumed to already be sorted the way the
/// caller wants (Loki returns each stream's `values` in timestamp order, and
/// entries here should already be merged across streams by the caller if global
/// ordering matters).
pub fn entries_to_batch(schema: &SchemaRef, label_schema: &LabelSchema, entries: &[LogEntry]) -> Result<RecordBatch> {
    let timestamps = TimestampNanosecondArray::from_iter_values(entries.iter().map(|e| e.timestamp_ns));
    let lines: StringArray = entries.iter().map(|e| Some(e.line.as_str())).collect();

    let mut columns: Vec<ArrayRef> = vec![Arc::new(timestamps), Arc::new(lines)];

    match label_schema {
        LabelSchema::Flattened(label_names) => {
            for label in label_names {
                let mut builder = StringBuilder::with_capacity(entries.len(), entries.len() * 16);
                for entry in entries {
                    match entry.labels.get(label) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
        LabelSchema::MapColumn => {
            let key_builder = StringBuilder::new();
            let value_builder = StringBuilder::new();
            let mut map_builder = MapBuilder::new(None, key_builder, value_builder);

            for entry in entries {
                for (k, v) in &entry.labels {
                    map_builder.keys().append_value(k);
                    map_builder.values().append_value(v);
                }
                map_builder.append(true)?;
            }
            columns.push(Arc::new(map_builder.finish()));
        }
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

/// Splits a slice of entries into row-chunks of at most `batch_size` and
/// converts each chunk into a `RecordBatch`.
pub fn entries_to_batches(
    schema: &SchemaRef,
    label_schema: &LabelSchema,
    entries: &[LogEntry],
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    if entries.is_empty() {
        return Ok(vec![]);
    }
    entries
        .chunks(batch_size.max(1))
        .map(|chunk| entries_to_batch(schema, label_schema, chunk))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::build_schema;
    use std::collections::BTreeMap;

    #[test]
    fn converts_flattened_entries() {
        let label_schema = LabelSchema::Flattened(vec!["job".to_string()]);
        let schema = Arc::new(build_schema(&label_schema));
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "myapp".to_string());
        let entries = vec![LogEntry {
            timestamp_ns: 1_700_000_000_000_000_000,
            line: "hello world".to_string(),
            labels,
        }];
        let batch = entries_to_batch(&schema, &label_schema, &entries).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn converts_map_entries() {
        let label_schema = LabelSchema::MapColumn;
        let schema = Arc::new(build_schema(&label_schema));
        let mut labels = BTreeMap::new();
        labels.insert("job".to_string(), "myapp".to_string());
        labels.insert("env".to_string(), "prod".to_string());
        let entries = vec![LogEntry {
            timestamp_ns: 1_700_000_000_000_000_000,
            line: "hello world".to_string(),
            labels,
        }];
        let batch = entries_to_batch(&schema, &label_schema, &entries).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }
}
