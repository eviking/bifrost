// Package plugin implements the Bifrost Grafana backend datasource.
//
// This plugin does not talk to Loki directly. It calls out to the
// bifrost-bridge HTTP service (a separate Rust process), which embeds the
// datafusion-loki TableProvider and actually runs the SQL through Apache
// DataFusion against Loki. This file is the Go-side glue: it forwards each
// panel's SQL query to the bridge and reshapes the JSON response into a
// Grafana data.Frame.
package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Datasource holds the configuration needed to reach the bifrost-bridge
// HTTP service for one configured Grafana datasource instance.
type Datasource struct {
	bridgeURL  string
	httpClient *http.Client
}

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// jsonData mirrors the datasource config fields set in Grafana's datasource
// settings UI (the "JSON Data" blob), i.e. the bridge's base URL.
type jsonData struct {
	BridgeURL string `json:"bridgeUrl"`
}

// NewDatasource is called by Grafana once per configured datasource
// instance, reading the settings the user entered in the datasource config
// page.
func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var jd jsonData
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &jd); err != nil {
			return nil, fmt.Errorf("parsing datasource jsonData: %w", err)
		}
	}
	bridgeURL := jd.BridgeURL
	if bridgeURL == "" {
		bridgeURL = "http://127.0.0.1:8090"
	}
	return &Datasource{
		bridgeURL:  bridgeURL,
		httpClient: &http.Client{Timeout: 25 * time.Second},
	}, nil
}

// Dispose is called before the instance is replaced/discarded.
func (d *Datasource) Dispose() {}

// queryModel is the per-panel query payload Grafana sends, matching the
// frontend query editor's `sql` field (see src/datasource/types.ts).
type queryModel struct {
	SQL string `json:"sql"`
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

// QueryData handles one batch of panel queries. Grafana can send several
// queries in one call (e.g. multiple panels refreshing together); each is
// executed independently against the bridge.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	for _, q := range req.Queries {
		response.Responses[q.RefID] = d.query(ctx, q)
	}

	return response, nil
}

func (d *Datasource) query(ctx context.Context, query backend.DataQuery) backend.DataResponse {
	var qm queryModel
	if err := json.Unmarshal(query.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("invalid query JSON: %v", err))
	}
	if qm.SQL == "" {
		return backend.ErrDataResponse(backend.StatusBadRequest, "query is missing required 'sql' field")
	}

	sql := applyTimeRangeMacros(qm.SQL, query.TimeRange)

	bridgeResp, err := d.callBridge(ctx, sql)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}

	frame, err := framesFromBridgeResponse(query.RefID, bridgeResp)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("converting bridge response to data frame: %v", err))
	}

	var resp backend.DataResponse
	resp.Frames = append(resp.Frames, frame)
	return resp
}

// applyTimeRangeMacros substitutes Grafana's standard SQL-datasource time
// macros with literal bounds from the panel's selected time range, following
// the same convention as Grafana's official Postgres/MySQL/ClickHouse SQL
// datasources. Without this, the "Last 6 hours" picker has no effect at
// all — the raw SQL a user writes is sent to the bridge completely
// unmodified, so a query with no explicit WHERE clause on timestamp ignores
// the picker entirely.
//
// Supported macros:
//   - $__timeFrom() / $__timeTo()      -> a quoted RFC3339 timestamp literal
//   - $__timeFilter(column)            -> "column >= '<from>' AND column <= '<to>'"
//
// Bifrost's timestamp-bound pushdown (see src/time_range.rs) recognizes
// string literals parsed via RFC3339, so these substitutions are pushed into
// Loki's start/end query params rather than merely filtering client-side.
func applyTimeRangeMacros(sql string, tr backend.TimeRange) string {
	from := tr.From.UTC().Format(time.RFC3339Nano)
	to := tr.To.UTC().Format(time.RFC3339Nano)

	sql = strings.ReplaceAll(sql, "$__timeFrom()", fmt.Sprintf("'%s'", from))
	sql = strings.ReplaceAll(sql, "$__timeTo()", fmt.Sprintf("'%s'", to))
	sql = replaceTimeFilterMacro(sql, from, to)

	return sql
}

// replaceTimeFilterMacro expands every $__timeFilter(<column>) occurrence.
// A simple scan-for-balanced-parens approach is used instead of a regex so
// arbitrary column expressions (e.g. containing commas or nested calls)
// aren't mis-parsed.
func replaceTimeFilterMacro(sql, from, to string) string {
	const marker = "$__timeFilter("
	var out strings.Builder
	rest := sql
	for {
		idx := strings.Index(rest, marker)
		if idx == -1 {
			out.WriteString(rest)
			break
		}
		out.WriteString(rest[:idx])
		afterMarker := rest[idx+len(marker):]
		closeIdx := strings.IndexByte(afterMarker, ')')
		if closeIdx == -1 {
			// Unbalanced macro; leave the rest untouched rather than guessing.
			out.WriteString(rest[idx:])
			break
		}
		column := strings.TrimSpace(afterMarker[:closeIdx])
		out.WriteString(fmt.Sprintf("%s >= '%s' AND %s <= '%s'", column, from, column, to))
		rest = afterMarker[closeIdx+1:]
	}
	return out.String()
}

func (d *Datasource) callBridge(ctx context.Context, sql string) (*bridgeResponse, error) {
	body, err := json.Marshal(map[string]string{"sql": sql})
	if err != nil {
		return nil, fmt.Errorf("marshaling query request: %w", err)
	}

	url := d.bridgeURL + "/query"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("building bridge request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := d.httpClient.Do(httpReq)
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
	return &br, nil
}

// framesFromBridgeResponse converts the bridge's {columns, rows} JSON shape
// into a single Grafana data.Frame, one typed data.Field per column. Column
// types are driven by the "type" tag the bridge attaches to each column
// (derived from the underlying Arrow DataType), not by sniffing values.
func framesFromBridgeResponse(refID string, br *bridgeResponse) (*data.Frame, error) {
	fields := make([]*data.Field, len(br.Columns))

	for colIdx, col := range br.Columns {
		switch col.Type {
		case "time":
			values := make([]time.Time, len(br.Rows))
			for rowIdx, row := range br.Rows {
				ms, ok := row[colIdx].(float64)
				if !ok {
					return nil, fmt.Errorf("column %q: expected numeric epoch-ms for time field, got %T", col.Name, row[colIdx])
				}
				values[rowIdx] = time.UnixMilli(int64(ms)).UTC()
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		case "number":
			values := make([]*float64, len(br.Rows))
			for rowIdx, row := range br.Rows {
				if row[colIdx] == nil {
					continue
				}
				f, ok := row[colIdx].(float64)
				if !ok {
					return nil, fmt.Errorf("column %q: expected numeric value, got %T", col.Name, row[colIdx])
				}
				values[rowIdx] = &f
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		case "bool":
			values := make([]*bool, len(br.Rows))
			for rowIdx, row := range br.Rows {
				if row[colIdx] == nil {
					continue
				}
				b, ok := row[colIdx].(bool)
				if !ok {
					return nil, fmt.Errorf("column %q: expected bool value, got %T", col.Name, row[colIdx])
				}
				values[rowIdx] = &b
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		default: // "string" and anything else falls back to text
			values := make([]*string, len(br.Rows))
			for rowIdx, row := range br.Rows {
				if row[colIdx] == nil {
					continue
				}
				s := fmt.Sprintf("%v", row[colIdx])
				values[rowIdx] = &s
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		}
	}

	frame := data.NewFrame(refID, fields...)
	return frame, nil
}

// CheckHealth is invoked when the user clicks "Save & test" on the
// datasource config page; it confirms the bridge process is reachable.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, d.bridgeURL+"/health", nil)
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("cannot reach bifrost-bridge at %s: %v", d.bridgeURL, err),
		}, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("bifrost-bridge at %s returned HTTP %d", d.bridgeURL, resp.StatusCode),
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "bifrost-bridge is reachable",
	}, nil
}
