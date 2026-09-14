// Package generate implements the generate_strategy tool.
// This file holds the repair state machine (spec §8): a bounded loop that
// drives the validator and lets the model retry generate_strategy up to
// maxRepairs times after the initial call.
package generate

import (
	"context"
	"errors"
	"fmt"

	"github.com/dora-network/bond-trading-strategies/internal/agent/tools/generate/validate"
)

// Diagnostic and Result alias the canonical types in the validate package
// (spec §19.3). The validate package is the single source of truth; these
// aliases keep the generate package's existing references working.
type Diagnostic = validate.Diagnostic
type Result = validate.Result

// Validator runs a build+vet+test pass over a file map. The
// container-backed implementation in internal/tools/generate/validate
// will satisfy it; tests use a fake.
type Validator interface {
	Validate(ctx context.Context, files map[string]string) (Result, error)
}

// Repairer drives the bounded repair loop for generate_strategy. The
// first call counts as attempt 0; up to maxRepairs additional calls are
// allowed. A failure on the (maxRepairs+1)th attempt exhausts the budget
// and Run returns the last result with ErrBudgetExhausted.
type Repairer struct {
	v          Validator
	maxRepairs int
	// OnRepair, if set, is invoked after every failed validation attempt
	// (i.e. between the initial call and any repair attempts). The
	// attempt number is 0-indexed: 0 is the initial call's failure,
	// 1 is the first repair's failure, etc. The result is the failed
	// validation. If OnRepair is nil, no callback fires.
	OnRepair func(attempt int, result Result)
}

// NewRepairer returns a repairer that allows up to maxRepairs repair
// attempts (in addition to the initial call).
func NewRepairer(v Validator, maxRepairs int) *Repairer {
	return &Repairer{v: v, maxRepairs: maxRepairs}
}

// ErrBudgetExhausted is returned when the repair budget is exhausted.
var ErrBudgetExhausted = errors.New("generate: repair budget exhausted")

// Run executes the validation+repair loop. Returns the last result, the
// total attempt count, and an error if the budget is exhausted or the
// validator returns an infrastructure failure.
func (r *Repairer) Run(ctx context.Context, files map[string]string) (Result, int, error) {
	var last Result
	// Loop runs maxRepairs+1 times (initial call + maxRepairs repairs).
	for attempt := range r.maxRepairs + 1 {
		res, err := r.v.Validate(ctx, files)
		if err != nil {
			return Result{}, attempt, fmt.Errorf("validator: %w", err)
		}
		if res.BuildOK && res.VetOK && res.TestsOK {
			return res, attempt + 1, nil
		}
		last = res
		if r.OnRepair != nil {
			r.OnRepair(attempt, res)
		}
	}
	return last, r.maxRepairs + 1, ErrBudgetExhausted
}
