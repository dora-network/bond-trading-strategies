// Package generate implements the generate_strategy tool.
// This file holds the repair state machine tests (spec §8).
package generate

import (
	"context"
	"strings"
	"testing"
)

// fakeValidator returns canned Result values from a slice, in order.
// Once the slice is exhausted it returns a bare failure. It satisfies the
// Validator interface without touching a container.
type fakeValidator struct {
	results []Result
	calls   int
	err     error              // if non-nil, every Validate call returns it (infrastructure failure)
	capture *map[string]string // if set, the files map passed to Validate is captured here
}

func (f *fakeValidator) Validate(_ context.Context, files map[string]string) (Result, error) {
	if f.capture != nil {
		*f.capture = files
	}
	if f.err != nil {
		return Result{}, f.err
	}
	idx := f.calls
	f.calls++
	if idx >= len(f.results) {
		return Result{}, nil
	}
	return f.results[idx], nil
}

func TestRepair_FirstAttemptSucceeds(t *testing.T) {
	v := &fakeValidator{results: []Result{{BuildOK: true, VetOK: true, TestsOK: true}}}
	r := NewRepairer(v, 2)
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	res, attempts, err := r.Run(t.Context(), files)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.BuildOK {
		t.Errorf("BuildOK: want true, got %+v", res)
	}
	if attempts != 1 {
		t.Errorf("attempts: want 1, got %d", attempts)
	}
}

func TestRepair_OneRepair(t *testing.T) {
	v := &fakeValidator{results: []Result{
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", File: "main.go", Line: 1, Message: "x"}}},
		{BuildOK: true, VetOK: true, TestsOK: true},
	}}
	r := NewRepairer(v, 2)
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	res, attempts, err := r.Run(t.Context(), files)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.BuildOK {
		t.Errorf("BuildOK: want true, got %+v", res)
	}
	if attempts != 2 {
		t.Errorf("attempts: want 2, got %d", attempts)
	}
}

func TestRepair_BudgetExhausted(t *testing.T) {
	v := &fakeValidator{results: []Result{
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", Message: "x"}}},
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", Message: "y"}}},
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", Message: "z"}}},
	}}
	r := NewRepairer(v, 2)
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	_, attempts, err := r.Run(t.Context(), files)
	if err == nil || !strings.Contains(err.Error(), "budget") {
		t.Fatalf("want budget error, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts: want 3 (initial + 2 repairs), got %d", attempts)
	}
}

// TestRepair_OnRepairCalled asserts OnRepair fires once per failed attempt
// with the 0-indexed attempt number and the failed Result.
func TestRepair_OnRepairCalled(t *testing.T) {
	v := &fakeValidator{results: []Result{
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", File: "main.go", Line: 1, Message: "x"}}},
		{BuildOK: true, VetOK: true, TestsOK: true},
	}}
	var got []int
	var gotResults []Result
	r := NewRepairer(v, 2)
	r.OnRepair = func(attempt int, result Result) {
		got = append(got, attempt)
		gotResults = append(gotResults, result)
	}
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	if _, _, err := r.Run(t.Context(), files); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("OnRepair calls: want 1, got %d", len(got))
	}
	if got[0] != 0 {
		t.Errorf("attempt: want 0, got %d", got[0])
	}
	if gotResults[0].BuildOK {
		t.Errorf("failed result BuildOK: want false, got true")
	}
}

// TestRepair_OnRepairNotCalledOnSuccess asserts OnRepair does not fire
// when the initial validation passes on the first attempt.
func TestRepair_OnRepairNotCalledOnSuccess(t *testing.T) {
	v := &fakeValidator{results: []Result{{BuildOK: true, VetOK: true, TestsOK: true}}}
	called := false
	r := NewRepairer(v, 2)
	r.OnRepair = func(attempt int, result Result) { called = true }
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	if _, _, err := r.Run(t.Context(), files); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if called {
		t.Errorf("OnRepair: want not called on success, got called")
	}
}

// TestRepair_OnRepairNilSafe asserts a nil OnRepair does not panic.
func TestRepair_OnRepairNilSafe(t *testing.T) {
	v := &fakeValidator{results: []Result{
		{BuildOK: false, Diagnostics: []Diagnostic{{Category: "compile", Message: "x"}}},
		{BuildOK: true, VetOK: true, TestsOK: true},
	}}
	r := NewRepairer(v, 2) // OnRepair left nil
	files := map[string]string{"main.go": "package main", "go.mod": "module x", "main_test.go": "package main"}
	if _, _, err := r.Run(t.Context(), files); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
