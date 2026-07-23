import React, { ChangeEvent } from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, RadioButtonGroup } from '@grafana/ui';

import { BifrostDataSourceOptions, QueryMode } from '../types';

type Props = DataSourcePluginOptionsEditorProps<BifrostDataSourceOptions>;

const QUERY_MODE_OPTIONS: Array<{ label: string; value: QueryMode; description: string }> = [
  {
    label: 'HTTP bridge',
    value: 'http',
    description: 'Query via the bifrost-bridge process over HTTP. Default, supported.',
  },
  {
    label: 'In-process (FFI)',
    value: 'ffi',
    description:
      'Query DataFusion in-process via datafusion-ffi, no bridge process. Experimental -- see grafana-plugin/README.md.',
  },
];

export function ConfigEditor({ options, onOptionsChange }: Props) {
  const queryMode: QueryMode = options.jsonData.queryMode ?? 'http';

  const onBridgeUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...options.jsonData, bridgeUrl: event.target.value },
    });
  };

  const onQueryModeChange = (value: QueryMode) => {
    onOptionsChange({
      ...options,
      jsonData: { ...options.jsonData, queryMode: value },
    });
  };

  const onFFILokiUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...options.jsonData, ffiLokiUrl: event.target.value },
    });
  };

  const onFFIStreamSelectorChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...options.jsonData, ffiStreamSelector: event.target.value },
    });
  };

  const onFFILabelsChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: { ...options.jsonData, ffiLabelsCsv: event.target.value },
    });
  };

  return (
    <div className="gf-form-group">
      <InlineField
        label="Query mode"
        labelWidth={20}
        tooltip="How this datasource reaches DataFusion. HTTP bridge is the default, supported path; In-process (FFI) is experimental."
      >
        <RadioButtonGroup options={QUERY_MODE_OPTIONS} value={queryMode} onChange={onQueryModeChange} />
      </InlineField>

      {queryMode === 'http' ? (
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
      ) : (
        <>
          <InlineField
            label="Loki URL"
            labelWidth={20}
            tooltip="Base URL of the Loki instance to query directly (no bridge process in this mode)."
          >
            <Input
              width={40}
              value={options.jsonData.ffiLokiUrl ?? ''}
              onChange={onFFILokiUrlChange}
              placeholder="http://localhost:3100"
            />
          </InlineField>
          <InlineField
            label="LogQL stream selector"
            labelWidth={20}
            tooltip='The base LogQL selector for the "logs" table, e.g. {job="myapp"}'
          >
            <Input
              width={40}
              value={options.jsonData.ffiStreamSelector ?? ''}
              onChange={onFFIStreamSelectorChange}
              placeholder='{job="myapp"}'
            />
          </InlineField>
          <InlineField
            label="Labels (comma-separated)"
            labelWidth={20}
            tooltip="Label names to expose as flattened SQL columns, e.g. job,level,env,pod"
          >
            <Input
              width={40}
              value={options.jsonData.ffiLabelsCsv ?? ''}
              onChange={onFFILabelsChange}
              placeholder="job,level,env,pod"
            />
          </InlineField>
        </>
      )}
    </div>
  );
}
