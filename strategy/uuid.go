package strategy

import "github.com/google/uuid"

// MustParseUUID converts a non-empty DORA asset/order-book ID string
// (which the upstream API hands us as a string) into a uuid.UUID.
// Empty input is treated as uuid.Nil so the live-run path can record
// a decision even if the asset ID lookup failed earlier; the row
// still preserves run_id + seq for forensics.
func MustParseUUID(id string) uuid.UUID {
	if id == "" {
		return uuid.Nil
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil
	}
	return parsed
}
