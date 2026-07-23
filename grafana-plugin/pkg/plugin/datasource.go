// Package plugin implements the Bifrost Grafana backend datasource.
//
// A datasource instance answers queries via one of two interchangeable
// queryEngine implementations, chosen by the "queryMode" datasource setting:
//
//   - "http" (default): calls out to bifrost-bridge, a separate Rust
//     process embedding datafusion-loki, over HTTP. See httpengine.go.
//   - "ffi": loads datafusion-loki's LokiTableProvider directly into this
//     process via datafusion-ffi/datafusion-go -- no separate bridge
//     process. Experimental; see pkg/lokiffi's package doc for why. See
//     ffiengine.go.
//
// Everything past engine selection (time-range macro substitution, frame
// building) is identical regardless of which engine answers a query.
package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
)

// Datasource holds the configured queryEngine for one Grafana datasource
// instance. All Grafana-facing logic (QueryData, CheckHealth) is engine
// agnostic; see queryEngine in engine.go.
type Datasource struct {
	engine queryEngine
}

var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// jsonData mirrors the datasource config fields set in Grafana's datasource
// settings UI (the "JSON Data" blob).
type jsonData struct {
	// BridgeURL is used when QueryMode is "http" (the default).
	BridgeURL string `json:"bridgeUrl"`

	// QueryMode selects the query engine: "http" (default) or "ffi".
	QueryMode string `json:"queryMode"`

	// The following three are used when QueryMode is "ffi", to build the
	// in-process LokiTableProvider directly (there's no bridge process to
	// carry this configuration for that mode). See bridge/src/main.rs's
	// LOKI_URL/LOKI_STREAM_SELECTOR env vars for the equivalent HTTP-mode
	// configuration, which lives on the bridge process instead.
	FFILokiURL        string `json:"ffiLokiUrl"`
	FFIStreamSelector string `json:"ffiStreamSelector"`
	FFILabelsCSV      string `json:"ffiLabelsCsv"`
}

const (
	queryModeHTTP = "http"
	queryModeFFI  = "ffi"
)

// NewDatasource is called by Grafana once per configured datasource
// instance, reading the settings the user entered in the datasource config
// page, and constructs whichever queryEngine that configuration selects.
func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var jd jsonData
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &jd); err != nil {
			return nil, fmt.Errorf("parsing datasource jsonData: %w", err)
		}
	}

	switch jd.QueryMode {
	case queryModeFFI:
		lokiURL := jd.FFILokiURL
		if lokiURL == "" {
			lokiURL = "http://localhost:3100"
		}
		streamSelector := jd.FFIStreamSelector
		if streamSelector == "" {
			streamSelector = `{job="myapp"}`
		}
		var labels []string
		for _, l := range strings.Split(jd.FFILabelsCSV, ",") {
			if l = strings.TrimSpace(l); l != "" {
				labels = append(labels, l)
			}
		}
		if len(labels) == 0 {
			labels = []string{"job", "level", "env", "pod"}
		}

		engine, err := newFFIEngine(ctx, lokiURL, streamSelector, labels)
		if err != nil {
			return nil, fmt.Errorf(
				"query mode is \"ffi\" but the in-process engine failed to initialize: %w "+
					"(this mode is experimental and requires libbifrost_ffi_export to be built "+
					"and loadable -- see ffi-export/README.md; consider switching Query mode "+
					"back to \"http\" if you don't need it)",
				err,
			)
		}
		return &Datasource{engine: engine}, nil

	default: // queryModeHTTP, and the empty string for pre-existing datasources
		bridgeURL := jd.BridgeURL
		if bridgeURL == "" {
			bridgeURL = "http://127.0.0.1:8090"
		}
		return &Datasource{engine: newHTTPBridgeEngine(bridgeURL)}, nil
	}
}

// Dispose is called before the instance is replaced/discarded.
func (d *Datasource) Dispose() {
	d.engine.Close()
}

// queryModel is the per-panel query payload Grafana sends, matching the
// frontend query editor's `sql` field (see src/datasource/types.ts).
type queryModel struct {
	SQL string `json:"sql"`
}

// QueryData handles one batch of panel queries. Grafana can send several
// queries in one call (e.g. multiple panels refreshing together); each is
// executed independently against the configured engine.
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

	result, err := d.engine.Query(ctx, sql)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
	}

	frame, err := framesFromEngineResult(query.RefID, result)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("converting query result to data frame: %v", err))
	}

	var resp backend.DataResponse
	resp.Frames = append(resp.Frames, frame)
	return resp
}

// applyTimeRangeMacros substitutes Grafana's standard SQL-datasource time
// macros with literal bounds from the panel's selected time range, following
// the same convention as Grafana's official Postgres/MySQL/ClickHouse SQL
// datasources. Without this, the "Last 6 hours" picker has no effect at
// all — the raw SQL a user writes is sent to the engine completely
// unmodified, so a query with no explicit WHERE clause on timestamp ignores
// the picker entirely.
//
// Supported macros:
//   - $__timeFrom() / $__timeTo()      -> a quoted RFC3339 timestamp literal
//   - $__timeFilter(column)            -> "column >= '<from>' AND column <= '<to>'"
//
// Bifrost's timestamp-bound pushdown (see src/time_range.rs) recognizes
// string literals parsed via RFC3339, so these substitutions are pushed into
// Loki's start/end query params rather than merely filtering client-side,
// regardless of which engine is in use.
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

// CheckHealth is invoked when the user clicks "Save & test" on the
// datasource config page; it confirms the configured engine is usable.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	if err := d.engine.CheckHealth(ctx); err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "query engine is reachable"}, nil
}
