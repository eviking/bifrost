package plugin

import "context"

// queryEngine is the seam between the Datasource's Grafana-facing logic and
// however a query actually gets answered. Two implementations exist:
//
//   - httpBridgeEngine (httpengine.go): calls out to bifrost-bridge over
//     HTTP. The default, supported path.
//   - ffiEngine (ffiengine.go): loads LokiTableProvider in-process via
//     datafusion-ffi/datafusion-go, no separate process involved.
//     Experimental -- see pkg/lokiffi's package doc for why.
//
// Both report results in the same engineColumn/engineRow shape so
// frames.go's conversion to a Grafana data.Frame doesn't need to know or
// care which engine answered a given query.
type queryEngine interface {
	// Query runs sql and returns column metadata plus rows.
	Query(ctx context.Context, sql string) (*engineResult, error)

	// CheckHealth reports whether the engine is currently able to serve
	// queries (bridge reachable / FFI session usable).
	CheckHealth(ctx context.Context) error

	// Close releases any resources (HTTP client has none; the FFI engine
	// must deregister its table and free the foreign provider). Called
	// once when the datasource instance is disposed.
	Close()
}

// engineColumn describes one result column. Type is one of
// "time" | "number" | "string" | "bool", matching what both engines'
// underlying type systems (the bridge's Arrow-derived JSON tags, the FFI
// engine's database/sql ColumnType mapping) normalize down to.
type engineColumn struct {
	Name string
	Type string
}

// engineResult is a fully-decoded query result: column metadata plus rows
// of already-Go-typed values (time.Time for "time" columns, float64 for
// "number", string for "string", bool for "bool", nil for SQL NULL).
type engineResult struct {
	Columns []engineColumn
	Rows    [][]any
}
