package plugin

import (
	"context"
	"fmt"

	"bifrost-datafusion-datasource/pkg/lokiffi"
)

// ffiEngine loads LokiTableProvider in-process via datafusion-ffi and
// queries it through datafusion-go's cgo bindings -- no bifrost-bridge HTTP
// process involved. Experimental: see pkg/lokiffi's package doc for the
// exact dependency/build requirements this needs at runtime, which are not
// satisfied by a normal `go build` of this plugin alone.
type ffiEngine struct {
	engine *lokiffi.Engine
}

var _ queryEngine = (*ffiEngine)(nil)

// newFFIEngine builds a LokiTableProvider for the given Loki URL/selector
// and loads it in-process. Returns an error (rather than panicking or
// silently falling back) if libbifrost_ffi_export isn't loadable, the
// DataFusion version handshake fails, or the initial provider registration
// fails -- callers should surface this clearly rather than pretend the
// engine is usable.
func newFFIEngine(ctx context.Context, lokiURL, streamSelector string, labels []string) (*ffiEngine, error) {
	engine, err := lokiffi.NewEngine(ctx, lokiURL, streamSelector, labels)
	if err != nil {
		return nil, fmt.Errorf("initializing in-process FFI engine: %w", err)
	}
	return &ffiEngine{engine: engine}, nil
}

func (e *ffiEngine) Query(ctx context.Context, sql string) (*engineResult, error) {
	result, err := e.engine.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	columns := make([]engineColumn, len(result.Columns))
	for i, c := range result.Columns {
		columns[i] = engineColumn{Name: c.Name, Type: c.Type}
	}
	return &engineResult{Columns: columns, Rows: result.Rows}, nil
}

func (e *ffiEngine) CheckHealth(ctx context.Context) error {
	return e.engine.CheckHealth(ctx)
}

func (e *ffiEngine) Close() {
	// lokiffi.Engine.Close wants a context for table deregistration; there's
	// no meaningful caller-supplied context at Dispose() time (see
	// Datasource.Dispose in datasource.go), so use a background context with
	// no deadline. This mirrors what bifrost-bridge does implicitly by just
	// exiting the process -- deregistration here is best-effort cleanup of
	// this process's own in-memory state, not a network call that can hang.
	e.engine.Close(context.Background())
}
