package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// httpBridgeEngine calls out to bifrost-bridge (a separate Rust process
// embedding datafusion-loki) over HTTP. This is the default, supported
// query engine -- see pkg/lokiffi for the experimental in-process
// alternative.
type httpBridgeEngine struct {
	bridgeURL  string
	httpClient *http.Client
}

var _ queryEngine = (*httpBridgeEngine)(nil)

func newHTTPBridgeEngine(bridgeURL string) *httpBridgeEngine {
	return &httpBridgeEngine{
		bridgeURL:  bridgeURL,
		httpClient: &http.Client{Timeout: 25 * time.Second},
	}
}

type bridgeColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type bridgeResponse struct {
	Columns []bridgeColumn  `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
}

type bridgeError struct {
	Error string `json:"error"`
}

func (e *httpBridgeEngine) Query(ctx context.Context, sql string) (*engineResult, error) {
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		return nil, fmt.Errorf("marshaling query request: %w", err)
	}

	url := e.bridgeURL + "/query"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("building bridge request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("calling bifrost-bridge at %s: %w", url, err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		var be bridgeError
		if decodeErr := json.NewDecoder(httpResp.Body).Decode(&be); decodeErr == nil && be.Error != "" {
			return nil, fmt.Errorf("bifrost-bridge returned an error: %s", be.Error)
		}
		return nil, fmt.Errorf("bifrost-bridge returned HTTP %d", httpResp.StatusCode)
	}

	var br bridgeResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&br); err != nil {
		return nil, fmt.Errorf("decoding bridge response: %w", err)
	}

	return bridgeResponseToEngineResult(&br)
}

// bridgeResponseToEngineResult converts the bridge's raw JSON shape into the
// engine-agnostic engineResult, most notably turning "time" columns' raw
// epoch-millisecond JSON numbers into time.Time so downstream frame-building
// code (frames.go) never needs to know which engine produced a result.
func bridgeResponseToEngineResult(br *bridgeResponse) (*engineResult, error) {
	columns := make([]engineColumn, len(br.Columns))
	for i, c := range br.Columns {
		columns[i] = engineColumn{Name: c.Name, Type: c.Type}
	}

	rows := make([][]any, len(br.Rows))
	for rowIdx, row := range br.Rows {
		converted := make([]any, len(row))
		for colIdx, v := range row {
			if v == nil {
				continue
			}
			if columns[colIdx].Type == "time" {
				ms, ok := v.(float64)
				if !ok {
					return nil, fmt.Errorf("column %q: expected numeric epoch-ms for time field, got %T", columns[colIdx].Name, v)
				}
				converted[colIdx] = time.UnixMilli(int64(ms)).UTC()
			} else {
				converted[colIdx] = v
			}
		}
		rows[rowIdx] = converted
	}

	return &engineResult{Columns: columns, Rows: rows}, nil
}

func (e *httpBridgeEngine) CheckHealth(ctx context.Context) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, e.bridgeURL+"/health", nil)
	if err != nil {
		return err
	}

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("cannot reach bifrost-bridge at %s: %w", e.bridgeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bifrost-bridge at %s returned HTTP %d", e.bridgeURL, resp.StatusCode)
	}
	return nil
}

func (e *httpBridgeEngine) Close() {}
