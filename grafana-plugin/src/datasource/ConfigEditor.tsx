import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input } from '@grafana/ui';

import { BifrostDataSourceOptions } from '../types';

type Props = DataSourcePluginOptionsEditorProps<BifrostDataSourceOptions>;

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const onBridgeUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...options.jsonData,
        bridgeUrl: event.target.value,
      },
    });
  };

  return (
    <div className="gf-form-group">
      <InlineField
        label="Bifrost bridge URL"
        labelWidth={20}
        tooltip="Base URL of the bifrost-bridge process (Rust/DataFusion/Loki), e.g. http://127.0.0.1:8090"
      >
        <Input
          width={40}
          value={options.jsonData.bridgeUrl ?? ''}
          onChange={onBridgeUrlChange}
          placeholder="http://127.0.0.1:8090"
        />
      </InlineField>
    </div>
  );
}
