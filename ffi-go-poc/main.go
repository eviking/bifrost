// Command ffi-go-poc proves that a Go process can query Grafana Loki through
// Apache DataFusion IN-PROCESS -- no separate bifrost-bridge HTTP server in
// between -- by loading LokiTableProvider across a C ABI boundary via
// datafusion-ffi and datafusion-go's cgo bindings.
//
// This is a prototype path, not what the real Grafana plugin (grafana-plugin/)
// uses today; see ffi-export/README.md and the root README's "In-process FFI
// prototype" section for the full setup, status, and why the HTTP bridge
// remains the supported path for now.
package main

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
	"log"
	"os"
	"unsafe"

	datafusion "github.com/datafusion-contrib/datafusion-go"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	lokiURL := getenv("LOKI_URL", "http://localhost:3100")
	streamSelector := getenv("LOKI_STREAM_SELECTOR", `{job="myapp"}`)
	labelsCSV := getenv("LOKI_LABELS", "job,level,env,pod")
	sqlQuery := getenv("QUERY", "SELECT level, COUNT(*) AS n FROM logs WHERE level = 'error' GROUP BY level")

	rustVersion := C.GoString(C.bifrost_ffi_datafusion_version())
	goVersion := datafusion.DataFusionVersion
	fmt.Println("bifrost-ffi-export DataFusion version:", rustVersion)
	fmt.Println("datafusion-go DataFusion version:      ", goVersion)
	if rustVersion != goVersion {
		log.Fatalf(
			"version mismatch: bifrost-ffi-export was built against DataFusion %s but "+
				"datafusion-go expects %s -- rebuild bifrost-ffi-export pinned to the same "+
				"version, see ffi-export/Cargo.toml",
			rustVersion, goVersion,
		)
	}

	cBaseURL := C.CString(lokiURL)
	defer C.free(unsafe.Pointer(cBaseURL))
	cStreamSelector := C.CString(streamSelector)
	defer C.free(unsafe.Pointer(cStreamSelector))
	cLabelsCSV := C.CString(labelsCSV)
	defer C.free(unsafe.Pointer(cLabelsCSV))

	provider := C.bifrost_ffi_create_provider(cBaseURL, cStreamSelector, cLabelsCSV)
	if provider == nil {
		log.Fatal("bifrost_ffi_create_provider returned NULL (check LOKI_URL/LOKI_STREAM_SELECTOR/LOKI_LABELS are valid UTF-8)")
	}
	defer C.bifrost_ffi_free_provider(provider)

	db, err := sql.Open("datafusion", "")
	if err != nil {
		log.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	table, err := datafusion.RegisterFFITableProvider(
		ctx, conn, "logs",
		unsafe.Pointer(provider),
		datafusion.DataFusionVersion,
	)
	if err != nil {
		log.Fatalf("RegisterFFITableProvider: %v", err)
	}
	defer table.Deregister(ctx)

	fmt.Println("=== running query in-process (no HTTP bridge) ===")
	fmt.Println(sqlQuery)
	rows, err := conn.QueryContext(ctx, sqlQuery)
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatalf("columns: %v", err)
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			log.Fatalf("scan: %v", err)
		}
		fmt.Println(vals)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("rows: %v", err)
	}

	fmt.Printf("OK: %d row(s) queried through DataFusion in-process, zero HTTP hops to a bridge process\n", rowCount)
}
