package http

import "testing"

func TestIsBotUser(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		firstName string
		lastName  string
		want      bool
	}{
		{"trader underscore prefix in first name", "TRADER_01", "Smith", true},
		{"mm underscore prefix in first name", "MM_Alice", "Brown", true},
		{"trader underscore prefix in last name", "Alice", "TRADER_99", true},
		{"mm prefix in last name", "Alice", "mm_bot", true},
		{"lowercase variants", "trader_42", "doe", true},
		{"no prefix in either", "Alice", "Smith", false},
		{"empty names", "", "", false},
		{"only first name no prefix", "Alice", "", false},
		{"only last name no prefix", "", "Smith", false},
		{"trader without underscore is not a bot", "Trader", "Smith", false},
		{"mm without underscore is not a bot", "Mm", "Smith", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isBotUser(tc.firstName, tc.lastName); got != tc.want {
				t.Errorf("isBotUser(%q, %q) = %v, want %v", tc.firstName, tc.lastName, got, tc.want)
			}
		})
	}
}
