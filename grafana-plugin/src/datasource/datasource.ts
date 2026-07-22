import { DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';

import { BifrostDataSourceOptions, BifrostQuery, DEFAULT_QUERY } from '../types';

// All actual query execution happens server-side in the Go backend
// (pkg/plugin/datasource.go), which forwards SQL to the bifrost-bridge Rust
// process. This class only needs to satisfy Grafana's frontend contract for
// a backend-driven datasource; DataSourceWithBackend handles routing
// QueryData calls to the plugin backend automatically.
export class DataSource extends DataSourceWithBackend<BifrostQuery, BifrostDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<BifrostDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(): Partial<BifrostQuery> {
    return DEFAULT_QUERY;
  }
}
