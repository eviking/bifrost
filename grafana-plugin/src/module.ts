import { DataSourcePlugin } from '@grafana/data';

import { DataSource } from './datasource/datasource';
import { ConfigEditor } from './datasource/ConfigEditor';
import { QueryEditor } from './datasource/QueryEditor';
import { BifrostDataSourceOptions, BifrostQuery } from './types';

export const plugin = new DataSourcePlugin<DataSource, BifrostQuery, BifrostDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
