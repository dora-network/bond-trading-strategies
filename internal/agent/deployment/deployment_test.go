package deployment

import "testing"

func TestPackage_Compiles(t *testing.T) {
	// Compile-time guard: types, constants, and interface are well-formed.
	_ = StatusRunning
	_ = Page{}
	_ = Deployment{}
	_ = Store(nil)
	_ = ErrNotFound
	_ = ErrAlreadyRunning
}
