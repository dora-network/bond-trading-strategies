package sanitize

import "testing"

func TestFilter_Oversize(t *testing.T) {
	prompt := make([]byte, MaxPromptBytes+1)
	for i := range prompt {
		prompt[i] = 'a'
	}
	verdict, err := Filter(string(prompt))
	if err == nil || verdict != RejectOversize {
		t.Fatalf("want RejectOversize, got verdict=%v err=%v", verdict, err)
	}
}

func TestFilter_ControlChars(t *testing.T) {
	verdict, err := Filter("hello\x00world")
	if err == nil || verdict != RejectControlChars {
		t.Fatalf("want RejectControlChars, got verdict=%v err=%v", verdict, err)
	}
}

func TestFilter_ControlChars_BOM(t *testing.T) {
	verdict, err := Filter("hello\uFEFFworld")
	if err == nil || verdict != RejectControlChars {
		t.Fatalf("want RejectControlChars, got verdict=%v err=%v", verdict, err)
	}
}

func TestFilter_Empty(t *testing.T) {
	verdict, err := Filter("   \n\t")
	if err == nil || verdict != RejectEmpty {
		t.Fatalf("want RejectEmpty, got verdict=%v err=%v", verdict, err)
	}
}

func TestFilter_OK(t *testing.T) {
	verdict, err := Filter("Build a bond strategy for BOND asset with size 1000")
	if err != nil || verdict != VerdictOK {
		t.Fatalf("want VerdictOK, got verdict=%v err=%v", verdict, err)
	}
}

func TestFilter_Category(t *testing.T) {
	for _, tc := range []struct {
		verdict Verdict
		want    string
	}{
		{VerdictOK, ""},
		{RejectOversize, "oversize"},
		{RejectControlChars, "control_chars"},
		{RejectEmpty, "empty"},
	} {
		if got := tc.verdict.Category(); got != tc.want {
			t.Errorf("%d.Category() = %q, want %q", tc.verdict, got, tc.want)
		}
	}
}
