// Package lokiffi is the in-process alternative to calling out to
// bifrost-bridge over HTTP: it loads LokiTableProvider directly into this
// process's memory via datafusion-ffi and queries it through datafusion-go's
// cgo bindings, with zero network hops to a separate bridge process.
//
// # Status
//
// This is the code path proven out in ../../../ffi-go-poc/. It depends on:
//
//  1. libbifrost_ffi_export.{dylib,so} being built (cargo build --release -p
//     bifrost-ffi-export at the repo root) and discoverable at runtime via
//     the platform's shared-library search path (DYLD_LIBRARY_PATH on
//     macOS, LD_LIBRARY_PATH on Linux, or an rpath baked in at link time).
//  2. datafusion-go's own native library, built from source against the
//     SAME unreleased commit this package's go.mod pins
//     (github.com/datafusion-contrib/datafusion-go), since the Go-side FFI
//     registration feature this package needs is not in any tagged
//     datafusion-go release yet. See ../../../ffi-export/README.md and
//     ../../../ffi-go-poc/README.md for exact setup steps and why.
//
// Because of (2), this package -- and therefore the "in-process (FFI)"
// query mode in the plugin's datasource config -- is experimental. The HTTP
// bridge mode (pkg/plugin/httpengine.go) is the supported default.
package lokiffi

/*
#cgo LDFLAGS: -lbifrost_ffi_export
#include <stdlib.h>

typedef struct FFI_TableProvider FFI_TableProvider;

FFI_TableProvider *bifrost_ffi_create_provider(const char *base_url, const char *stream_selector, const char *labels_csv);
void bifrost_ffi_free_provider(FFI_TableProvider *provider);
const char *bifrost_ffi_datafusion_version(void);
*/
import "C"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"unsafe"

	datafusion "github.com/datafusion-contrib/datafusion-go"
)

// Column mirrors the shape the HTTP bridge returns in its {columns, rows}
// JSON response, so callers (pkg/plugin) can build a Grafana data.Frame from
// either query engine identically.
type Column struct {
	Name string
	Type string // "time" | "number" | "string" | "bool"
}

// Result is the FFI-engine equivalent of decoding the bridge's JSON
// response: column metadata plus rows of already-Go-typed values.
type Result struct {
	Columns []Column
	Rows    [][]any
}

// Engine owns one in-process DataFusion session with a single "logs" table
// registered against it, backed by a LokiTableProvider loaded via FFI.
// Safe for concurrent use; database/sql's *sql.DB pools connections
// internally.
type Engine struct {
	db       *sql.DB
	provider *C.FFI_TableProvider // owned by this Engine
	table    *datafusion.RegisteredTable

	closeOnce sync.Once
}

// DataFusionVersion returns bifrost-ffi-export's compiled-in DataFusion
// version. NewEngine checks this against datafusion.DataFusionVersion
// (datafusion-go's own compiled-in version) before registering anything --
// see that check's comment for why a mismatch must be treated as fatal
// rather than attempted anyway.
func DataFusionVersion() string {
	return C.GoString(C.bifrost_ffi_datafusion_version())
}

// NewEngine builds a LokiTableProvider for the given Loki base URL and LogQL
// stream selector, loads it in-process via FFI, and registers it as a table
// named "logs" against a fresh DataFusion session -- mirroring exactly what
// bifrost-bridge's main.rs does for the HTTP path, just without the HTTP
// server in between.
func NewEngine(ctx context.Context, lokiURL, streamSelector string, labels []string) (*Engine, error) {
	rustVersion := DataFusionVersion()
	goVersion := datafusion.DataFusionVersion
	if rustVersion != goVersion {
		return nil, fmt.Errorf(
			"lokiffi: DataFusion version mismatch: bifrost-ffi-export was built against %q "+
				"but datafusion-go expects %q -- these must match exactly (DataFusion's FFI ABI "+
				"is not stable across versions pre-1.0); rebuild bifrost-ffi-export pinned to "+
				"datafusion-go's version",
			rustVersion, goVersion,
		)
	}

	cBaseURL := C.CString(lokiURL)
	defer C.free(unsafe.Pointer(cBaseURL))
	cStreamSelector := C.CString(streamSelector)
	defer C.free(unsafe.Pointer(cStreamSelector))
	cLabelsCSV := C.CString(strings.Join(labels, ","))
	defer C.free(unsafe.Pointer(cLabelsCSV))

	provider := C.bifrost_ffi_create_provider(cBaseURL, cStreamSelector, cLabelsCSV)
	if provider == nil {
		return nil, fmt.Errorf("lokiffi: bifrost_ffi_create_provider returned NULL (check lokiURL/streamSelector/labels are valid UTF-8)")
	}

	db, err := sql.Open("datafusion", "")
	if err != nil {
		C.bifrost_ffi_free_provider(provider)
		return nil, fmt.Errorf("lokiffi: sql.Open: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		C.bifrost_ffi_free_provider(provider)
		return nil, fmt.Errorf("lokiffi: db.Conn: %w", err)
	}
	defer conn.Close()

	table, err := datafusion.RegisterFFITableProvider(
		ctx, conn, "logs",
		unsafe.Pointer(provider),
		datafusion.DataFusionVersion,
	)
	if err != nil {
		db.Close()
		C.bifrost_ffi_free_provider(provider)
		return nil, fmt.Errorf("lokiffi: RegisterFFITableProvider: %w", err)
	}

	return &Engine{db: db, provider: provider, table: table}, nil
}

// Query runs sql against the in-process session and returns results in the
// same shape the HTTP bridge's JSON response decodes to.
func (e *Engine) Query(ctx context.Context, query string) (*Result, error) {
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("lokiffi: query: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("lokiffi: column types: %w", err)
	}

	columns := make([]Column, len(colTypes))
	for i, ct := range colTypes {
		columns[i] = Column{Name: ct.Name(), Type: mapColumnType(ct)}
	}

	var result Result
	result.Columns = columns

	scanDest := make([]any, len(columns))
	scanPtrs := make([]any, len(columns))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, fmt.Errorf("lokiffi: scan: %w", err)
		}
		row := make([]any, len(columns))
		copy(row, scanDest)
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lokiffi: rows: %w", err)
	}

	return &result, nil
}

// CheckHealth runs a trivial query to confirm the in-process session and
// registered provider are actually usable, mirroring what the HTTP bridge's
// /health endpoint confirms for that path.
func (e *Engine) CheckHealth(ctx context.Context) error {
	_, err := e.Query(ctx, "SELECT 1")
	return err
}

// Close deregisters the table and releases the FFI provider. Safe to call
// multiple times; only the first call has effect.
func (e *Engine) Close(ctx context.Context) {
	e.closeOnce.Do(func() {
		if e.table != nil {
			_ = e.table.Deregister(ctx)
		}
		if e.db != nil {
			_ = e.db.Close()
		}
		if e.provider != nil {
			C.bifrost_ffi_free_provider(e.provider)
		}
	})
}

// mapColumnType maps a database/sql ColumnType's DatabaseTypeName() to the
// same "time"/"number"/"string"/"bool" vocabulary the HTTP bridge's
// arrow_type_name (bridge/src/main.rs) uses, so pkg/plugin's
// framesFromBridgeResponse-equivalent logic can treat both engines
// identically regardless of which one answered the query.
//
// datafusion-go derives these names directly from Arrow type names
// (rows.go, func databaseTypeName) rather than SQL-style type names: plain
// scalar types report their lowercased Arrow name uppercased (e.g. INT64,
// FLOAT64, UTF8, BOOL), while parameterized types like timestamps report a
// bracketed suffix (e.g. "TIMESTAMP[ns, tz=UTC]", "TIMESTAMP[ns]") --
// checking a Has-prefix on "TIMESTAMP" covers both the tz and no-tz forms.
func mapColumnType(ct *sql.ColumnType) string {
	name := strings.ToUpper(ct.DatabaseTypeName())
	switch {
	case strings.HasPrefix(name, "TIMESTAMP"):
		return "time"
	case strings.HasPrefix(name, "INT") || strings.HasPrefix(name, "UINT") ||
		strings.HasPrefix(name, "FLOAT") || strings.HasPrefix(name, "DECIMAL"):
		return "number"
	case name == "BOOL" || name == "BOOLEAN":
		return "bool"
	default:
		return "string"
	}
}
