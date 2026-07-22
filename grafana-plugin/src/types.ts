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

export interface BifrostDataSourceOptions extends DataSourceJsonData {
  bridgeUrl?: string;
}
