import React, { ChangeEvent } from 'react';
import { QueryEditorProps } from '@grafana/data';
import { TextArea, InlineField } from '@grafana/ui';

import { DataSource } from './datasource';
import { BifrostDataSourceOptions, BifrostQuery } from '../types';

type Props = QueryEditorProps<DataSource, BifrostQuery, BifrostDataSourceOptions>;

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onSqlChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, sql: event.target.value });
  };

  return (
    <div className="gf-form-group">
      <InlineField
        label="SQL"
        labelWidth={10}
        grow
        tooltip="SQL executed by Apache DataFusion against the Bifrost Loki table provider. Use $__timeFilter(timestamp) in WHERE to apply the dashboard's selected time range — without it, the time picker has no effect on results."
      >
        <TextArea
          value={query.sql ?? ''}
          onChange={onSqlChange}
          onBlur={onRunQuery}
          rows={8}
          placeholder="SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count FROM logs WHERE $__timeFilter(timestamp) GROUP BY time, level ORDER BY time"
        />
      </InlineField>
      <div className="gf-form-help-icon" style={{ fontSize: 12, color: '#8e8e8e', marginTop: 4 }}>
        Tip: add <code>WHERE $__timeFilter(timestamp)</code> so the dashboard time picker (e.g.
        &quot;Last 6 hours&quot;) actually constrains the query — it has no effect otherwise.
      </div>
    </div>
  );
}
