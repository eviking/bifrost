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
      <InlineField label="SQL" labelWidth={10} grow tooltip="SQL executed by Apache DataFusion against the Bifrost Loki table provider.">
        <TextArea
          value={query.sql ?? ''}
          onChange={onSqlChange}
          onBlur={onRunQuery}
          rows={8}
          placeholder="SELECT date_trunc('minute', timestamp) AS time, level, COUNT(*) AS count FROM logs GROUP BY time, level ORDER BY time"
        />
      </InlineField>
    </div>
  );
}
