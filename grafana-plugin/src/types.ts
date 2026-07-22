import type { DataSourceJsonData } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';

export interface BifrostQuery extends DataQuery {
  sql?: string;
}

export const DEFAULT_QUERY: Partial<BifrostQuery> = {
  sql: "SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count\nFROM logs\nGROUP BY time, level\nORDER BY time",
};

export interface BifrostDataSourceOptions extends DataSourceJsonData {
  bridgeUrl?: string;
}
