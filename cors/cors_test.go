package cors_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/dora-network/bond-trading-strategies/cors"
)

func TestNew_EmptyOrigins_PassesThrough(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://anywhere.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called when no origins configured")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("expected no Allow-Origin header, got %q", got)
	}
}

func TestNew_Wildcard_WithOrigin_EchoesOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called for GET")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q, want echo of Origin", got)
	}
	if got := rr.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Errorf("Vary = %q, want it to contain Origin", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Errorf("Allow-Credentials = %q, want true", got)
	}
}

func TestNew_Wildcard_WithoutOrigin_LiteralStar(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}
}

func TestNew_ExplicitList_MatchingOrigin_Echoes(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com,https://b.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://a.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://a.com" {
		t.Errorf("Allow-Origin = %q, want echo of Origin", got)
	}
	if got := rr.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Errorf("Vary = %q, want it to contain Origin", got)
	}
}

func TestNew_ExplicitList_NonMatchingOrigin_NoAllowOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com,https://b.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://attacker.com")
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called (CORS headers do not block server-side)")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Allow-Origin = %q, want empty (non-matching origin)", got)
	}
}

func TestNew_ExplicitList_NoOriginHeader_NoAllowOrigin(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("https://a.com")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Allow-Origin = %q, want empty (no Origin header)", got)
	}
}

func TestNew_OptionsPreflight_ShortCircuitsWith204(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://app.example.com")
	h.ServeHTTP(rr, req)

	if called {
		t.Error("expected next handler NOT to be called for OPTIONS preflight")
	}
	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "GET") {
		t.Errorf("Allow-Methods = %q, want it to contain GET", got)
	}
}

func TestNew_AllowedHeaders_IncludeWebSocketHeaders(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true })

	h := cors.New("*")(next)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	if !called {
		t.Fatal("expected next handler to be called")
	}
	got := rr.Header().Get("Access-Control-Allow-Headers")
	for _, want := range []string{"Authorization", "Content-Type", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
		if !strings.Contains(got, want) {
			t.Errorf("Allow-Headers = %q, want it to contain %q", got, want)
		}
	}
}

func TestNew_AllowedMethods_IncludePatch(t *testing.T) {
	h := cors.New("*")(http.NewServeMux())
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	h.ServeHTTP(rr, req)

	got := rr.Header().Get("Access-Control-Allow-Methods")
	if !strings.Contains(got, "PATCH") {
		t.Errorf("Allow-Methods = %q, want it to contain PATCH", got)
	}
}

func TestOriginPatterns_Star_AllowAllNoPatterns(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("*")
	if !allowAll {
		t.Error("expected allowAll=true for *")
	}
	if len(patterns) != 0 {
		t.Errorf("expected no patterns for *, got %v", patterns)
	}
}

func TestOriginPatterns_EmptyInput_NoPatternsNotAllowed(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("")
	if allowAll {
		t.Error("expected allowAll=false for empty input")
	}
	if len(patterns) != 0 {
		t.Errorf("expected no patterns for empty input, got %v", patterns)
	}
}

func TestOriginPatterns_SingleURL_StripsScheme(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("https://app.example.com")
	if allowAll {
		t.Error("expected allowAll=false for explicit URL")
	}
	want := []string{"app.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_URLWithPort_PreservesPort(t *testing.T) {
	patterns, _ := cors.OriginPatterns("http://app.example.com:8080")
	want := []string{"app.example.com:8080"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_GlobPattern_KeepsWildcard(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("*.example.com")
	if allowAll {
		t.Error("expected allowAll=false for glob")
	}
	want := []string{"*.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_URLWithGlob_StripsSchemeKeepsWildcard(t *testing.T) {
	patterns, _ := cors.OriginPatterns("https://*.example.com")
	want := []string{"*.example.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_MultipleEntries_AllInOrder(t *testing.T) {
	patterns, _ := cors.OriginPatterns("https://a.com,https://b.com,*.c.com")
	want := []string{"a.com", "b.com", "*.c.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_WhitespaceAndEmptyEntries_Skipped(t *testing.T) {
	patterns, _ := cors.OriginPatterns(" https://a.com , , https://b.com ")
	want := []string{"a.com", "b.com"}
	if !equalStrings(patterns, want) {
		t.Errorf("patterns = %v, want %v", patterns, want)
	}
}

func TestOriginPatterns_StarMixedWithEntries_AllowAllTrue(t *testing.T) {
	patterns, allowAll := cors.OriginPatterns("https://a.com,*")
	if !allowAll {
		t.Error("expected allowAll=true when * is present")
	}
	if len(patterns) == 0 {
		t.Error("expected patterns to be populated (caller decides which to use)")
	}
}

func equalStrings(a, b []string) bool {
	return slices.Equal(a, b)
}
