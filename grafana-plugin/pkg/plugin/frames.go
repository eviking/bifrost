package plugin

import (
	"fmt"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// framesFromEngineResult converts an engine-agnostic query result into a
// single Grafana data.Frame, one typed data.Field per column. Column types
// are driven by the engine's own "time"/"number"/"string"/"bool"
// classification (each engine normalizes its native type system down to
// this vocabulary before returning an engineResult -- see
// bridgeResponseToEngineResult in httpengine.go and mapColumnType in
// pkg/lokiffi), not by sniffing values here.
func framesFromEngineResult(refID string, result *engineResult) (*data.Frame, error) {
	fields := make([]*data.Field, len(result.Columns))

	for colIdx, col := range result.Columns {
		switch col.Type {
		case "time":
			values := make([]time.Time, len(result.Rows))
			for rowIdx, row := range result.Rows {
				t, ok := row[colIdx].(time.Time)
				if !ok {
					return nil, fmt.Errorf("column %q: expected time.Time for time field, got %T", col.Name, row[colIdx])
				}
				values[rowIdx] = t
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		case "number":
			values := make([]*float64, len(result.Rows))
			for rowIdx, row := range result.Rows {
				v := row[colIdx]
				if v == nil {
					continue
				}
				f, ok := toFloat64(v)
				if !ok {
					return nil, fmt.Errorf("column %q: expected numeric value, got %T", col.Name, v)
				}
				values[rowIdx] = &f
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		case "bool":
			values := make([]*bool, len(result.Rows))
			for rowIdx, row := range result.Rows {
				v := row[colIdx]
				if v == nil {
					continue
				}
				b, ok := v.(bool)
				if !ok {
					return nil, fmt.Errorf("column %q: expected bool value, got %T", col.Name, v)
				}
				values[rowIdx] = &b
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		default: // "string" and anything else falls back to text
			values := make([]*string, len(result.Rows))
			for rowIdx, row := range result.Rows {
				v := row[colIdx]
				if v == nil {
					continue
				}
				s := fmt.Sprintf("%v", v)
				values[rowIdx] = &s
			}
			fields[colIdx] = data.NewField(col.Name, nil, values)
		}
	}

	return data.NewFrame(refID, fields...), nil
}

// toFloat64 widens the handful of concrete numeric Go types the two engines
// can produce for a "number" column (the HTTP bridge's JSON decoder always
// yields float64; the FFI engine's database/sql driver yields int64 for
// integer Arrow columns and float64 for floating-point ones) into a single
// float64, since Grafana's numeric field type is float64 regardless of the
// source column's original width/signedness.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}
