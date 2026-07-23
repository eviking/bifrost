import type { DataSourceJsonData } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';

export interface BifrostQuery extends DataQuery {
  sql?: string;
}

// $__timeFilter(timestamp) expands to the dashboard's selected time range
// (e.g. "Last 6 hours") before the query reaches the bridge — see
// applyTimeRangeMacros in pkg/plugin/datasource.go. Without it, the picker
// has no effect on results at all.
export const DEFAULT_QUERY: Partial<BifrostQuery> = {
  sql:
    "SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count\n" +
    'FROM logs\n' +
    'WHERE $__timeFilter(timestamp)\n' +
    'GROUP BY time, level\n' +
    'ORDER BY time',
};

export type QueryMode = 'http' | 'ffi';

export interface BifrostDataSourceOptions extends DataSourceJsonData {
  // Used when queryMode is "http" (the default).
  bridgeUrl?: string;

  // "http": query via bifrost-bridge over HTTP (default, supported).
  // "ffi": query in-process via datafusion-ffi/datafusion-go, no bridge
  // process. Experimental -- see grafana-plugin/README.md.
  queryMode?: QueryMode;

  // Used when queryMode is "ffi", to build the in-process LokiTableProvider
  // directly (there's no bridge process to carry this configuration for
  // that mode).
  ffiLokiUrl?: string;
  ffiStreamSelector?: string;
  ffiLabelsCsv?: string;
}
