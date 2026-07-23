package plugin

import (
	"testing"
	"time"
)

func TestFramesFromEngineResult_NumberColumnAcceptsIntAndFloat(t *testing.T) {
	// The HTTP bridge engine always produces float64 for "number" columns
	// (JSON has no separate integer type); the FFI engine produces int64
	// for integer Arrow columns and float64 for floating-point ones. Both
	// must convert cleanly.
	result := &engineResult{
		Columns: []engineColumn{{Name: "n", Type: "number"}},
		Rows: [][]any{
			{int64(42)},
			{float64(3.5)},
			{nil},
		},
	}

	frame, err := framesFromEngineResult("A", result)
	if err != nil {
		t.Fatalf("framesFromEngineResult: %v", err)
	}
	if frame.Fields[0].Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", frame.Fields[0].Len())
	}

	v0, _ := frame.Fields[0].At(0).(*float64)
	if v0 == nil || *v0 != 42 {
		t.Errorf("row 0: expected 42, got %v", v0)
	}
	v1, _ := frame.Fields[0].At(1).(*float64)
	if v1 == nil || *v1 != 3.5 {
		t.Errorf("row 1: expected 3.5, got %v", v1)
	}
	v2, _ := frame.Fields[0].At(2).(*float64)
	if v2 != nil {
		t.Errorf("row 2: expected nil, got %v", v2)
	}
}

func TestFramesFromEngineResult_TimeColumn(t *testing.T) {
	ts := time.Date(2026, 7, 23, 16, 12, 0, 0, time.UTC)
	result := &engineResult{
		Columns: []engineColumn{{Name: "time", Type: "time"}},
		Rows:    [][]any{{ts}},
	}

	frame, err := framesFromEngineResult("A", result)
	if err != nil {
		t.Fatalf("framesFromEngineResult: %v", err)
	}
	got, ok := frame.Fields[0].At(0).(time.Time)
	if !ok || !got.Equal(ts) {
		t.Errorf("expected %v, got %v", ts, got)
	}
}

func TestFramesFromEngineResult_StringAndBoolColumns(t *testing.T) {
	result := &engineResult{
		Columns: []engineColumn{
			{Name: "s", Type: "string"},
			{Name: "b", Type: "bool"},
		},
		Rows: [][]any{
			{"error", true},
			{nil, nil},
		},
	}

	frame, err := framesFromEngineResult("A", result)
	if err != nil {
		t.Fatalf("framesFromEngineResult: %v", err)
	}

	s0, _ := frame.Fields[0].At(0).(*string)
	if s0 == nil || *s0 != "error" {
		t.Errorf("row 0 string: expected \"error\", got %v", s0)
	}
	b0, _ := frame.Fields[1].At(0).(*bool)
	if b0 == nil || *b0 != true {
		t.Errorf("row 0 bool: expected true, got %v", b0)
	}
	s1, _ := frame.Fields[0].At(1).(*string)
	if s1 != nil {
		t.Errorf("row 1 string: expected nil, got %v", s1)
	}
}

func TestFramesFromEngineResult_RejectsWrongTypeForNumberColumn(t *testing.T) {
	result := &engineResult{
		Columns: []engineColumn{{Name: "n", Type: "number"}},
		Rows:    [][]any{{"not a number"}},
	}
	if _, err := framesFromEngineResult("A", result); err == nil {
		t.Error("expected an error for a non-numeric value in a \"number\" column, got nil")
	}
}

func TestBridgeResponseToEngineResult_ConvertsEpochMillisToTime(t *testing.T) {
	br := &bridgeResponse{
		Columns: []bridgeColumn{{Name: "time", Type: "time"}, {Name: "n", Type: "number"}},
		Rows: [][]interface{}{
			{float64(1784823120000), float64(23)},
		},
	}

	result, err := bridgeResponseToEngineResult(br)
	if err != nil {
		t.Fatalf("bridgeResponseToEngineResult: %v", err)
	}

	got, ok := result.Rows[0][0].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time, got %T", result.Rows[0][0])
	}
	want := time.UnixMilli(1784823120000).UTC()
	if !got.Equal(want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}
