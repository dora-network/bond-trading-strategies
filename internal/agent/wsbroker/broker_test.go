package wsbroker_test

import (
	"context"
	"encoding/json"

	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/dora-network/bond-trading-strategies/internal/agent/wsbroker"
)

func TestBroker_ConnectsAndReceivesFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "test done")
		_, _, _ = c.Read(r.Context())
		msg := map[string]any{"kind": "notification", "path": "/charts/candles", "data": map[string]any{
			"resolution": "1m", "candles": map[string]any{"OB-1": []map[string]string{
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T11:59:00Z", "open": "100", "high": "100.5", "low": "99.5", "close": "100", "volume": "5"},
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T12:00:00Z", "open": "100.5", "high": "101", "low": "100", "close": "100.5", "volume": "10"},
			}},
		}}
		b, _ := json.Marshal(msg)
		_ = c.Write(r.Context(), websocket.MessageText, b)
		time.Sleep(50 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe("OB-1", "1m", nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	select {
	case f := <-sub.Chan():
		if f.Type != "candle" || f.OrderBookID != "OB-1" {
			t.Errorf("got frame %+v, want OB-1 candle", f)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for candle frame")
	}
}

func TestBroker_ReconnectsAfterDrop(t *testing.T) {
	// Two-server setup: first server accepts and immediately closes
	// the connection; second server accepts and stays up.
	var mu sync.Mutex
	connects := 0

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		connects++
		mu.Unlock()
		c, _ := websocket.Accept(w, r, nil)
		_ = c.Close(websocket.StatusGoingAway, "test drop")
	}))
	t.Cleanup(srv1.Close)

	// A broker pointed at a server that immediately drops should retry
	// with backoff. We expect at least 2 connection attempts within 1s.
	wsURL := "ws" + strings.TrimPrefix(srv1.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{
		URL:              wsURL,
		APIKey:           "test-key",
		ReconnectBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })

	// Wait until the broker has reconnected at least twice.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		got := connects
		mu.Unlock()
		if got >= 2 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected at least 2 connection attempts, got %d", got)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestBroker_Subscribe_RoutesByOrderBook(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "test done")
		_, _, _ = c.Read(r.Context())
		msg := map[string]any{"kind": "notification", "path": "/charts/candles", "data": map[string]any{"resolution": "1m", "candles": map[string]any{
			"OB-1": []map[string]string{
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T12:00:00Z", "close": "100"},
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T12:01:00Z", "close": "100.5"},
			},
			"OB-2": []map[string]string{
				{"order_book_id": "OB-2", "start_timestamp": "2026-08-08T12:00:00Z", "close": "200"},
				{"order_book_id": "OB-2", "start_timestamp": "2026-08-08T12:01:00Z", "close": "200.5"},
			},
		}}}
		b, _ := json.Marshal(msg)
		_ = c.Write(r.Context(), websocket.MessageText, b)
		time.Sleep(50 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)
	b, err := wsbroker.New(wsbroker.Config{URL: "ws" + strings.TrimPrefix(srv.URL, "http"), APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	sub := b.Subscribe("OB-1", "1m", nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	select {
	case f := <-sub.Chan():
		if f.OrderBookID != "OB-1" {
			t.Errorf("got order book %q, want OB-1", f.OrderBookID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for OB-1 frame")
	}
	select {
	case f := <-sub.Chan():
		t.Errorf("unexpected second frame: %+v", f)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBroker_Subscribe_RoutesByChannelType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "test done")
		_, _, _ = c.Read(r.Context())
		msg := map[string]any{"kind": "notification", "path": "/charts/candles", "data": map[string]any{"resolution": "1m", "candles": map[string]any{
			"OB-1": []map[string]string{
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T12:00:00Z", "close": "100"},
				{"order_book_id": "OB-1", "start_timestamp": "2026-08-08T12:01:00Z", "close": "100.5"},
			},
		}}}
		b, _ := json.Marshal(msg)
		_ = c.Write(r.Context(), websocket.MessageText, b)
		time.Sleep(50 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)
	b, err := wsbroker.New(wsbroker.Config{URL: "ws" + strings.TrimPrefix(srv.URL, "http"), APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	sub := b.Subscribe("OB-1", "1m", []string{"candle"})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	select {
	case f := <-sub.Chan():
		if f.Type != "candle" {
			t.Errorf("got type %q, want candle", f.Type)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for candle frame")
	}
}

func TestBroker_Unsubscribe_ClosesChannel(t *testing.T) {
	// Single-frame server: push one frame, then close so the broker
	// exits its read loop.
	frames := []map[string]any{
		{"type": "price", "order_book_id": "OB-1"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "test done")
		for _, f := range frames {
			b, _ := json.Marshal(f)
			if err := c.Write(r.Context(), websocket.MessageText, b); err != nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	sub := b.Subscribe("OB-1", "1m", nil)
	// Unsubscribe before the broker starts so we exercise the
	// close path without race conditions.
	b.Unsubscribe(sub)
	// Idempotent: a second call must not panic on close-of-closed.
	b.Unsubscribe(sub)

	// Channel must be closed.
	select {
	case _, ok := <-sub.Chan():
		if ok {
			t.Errorf("expected channel closed, got a frame")
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expected channel closed within 100ms")
	}
}

// TestBroker_Dedup_SkipsSameStartTimestamp confirms two
// notifications carrying the same start_timestamp produce only
// one frame on the subscription channel.
func TestBroker_Dedup_SkipsSameStartTimestamp(t *testing.T) {
	ob, res := "OB-1", "1m"
	candles := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T11:59:00Z", "open": "99", "high": "100", "low": "98", "close": "99.5", "volume": "10"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:00:00Z", "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "10"},
	}
	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe(ob, res, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}
	push(res, map[string][]map[string]string{ob: candles})
	push(res, map[string][]map[string]string{ob: candles})

	select {
	case f := <-sub.Chan():
		if f.OrderBookID != ob || f.Resolution != res {
			t.Fatalf("got frame %+v, want %s/%s", f, ob, res)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first candle frame")
	}
	select {
	case f, ok := <-sub.Chan():
		if ok {
			t.Errorf("expected dedup to drop duplicate, got frame %+v", f)
		}
	case <-time.After(150 * time.Millisecond):
		// expected: no second frame
	}
}

// TestBroker_Dedup_ForwardsOnStartTimestampChange confirms that
// snapshot-style notifications with different start_timestamps
// both reach the subscriber.
func TestBroker_Dedup_ForwardsOnStartTimestampChange(t *testing.T) {
	ob, res := "OB-1", "1m"
	// Bootstrap snapshot: [previous closed, current in-progress].
	c1 := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:00:00Z", "close": "100"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "101"},
	}
	// New window: snapshot of [12:01 closed, 12:02 in-progress].
	c2 := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "101"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:02:00Z", "close": "102"},
	}
	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe(ob, res, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}
	push(res, map[string][]map[string]string{ob: c1})
	push(res, map[string][]map[string]string{ob: c2})

	seen := map[string]bool{}
	deadline := time.After(1 * time.Second)
	for {
		select {
		case f := <-sub.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			seen[meta.StartTimestamp] = true
		case <-deadline:
			goto done
		}
		if seen["2026-08-08T12:00:00Z"] && seen["2026-08-08T12:01:00Z"] {
			break
		}
	}
done:
	if !seen["2026-08-08T12:00:00Z"] || !seen["2026-08-08T12:01:00Z"] {
		t.Errorf("expected 12:00 and 12:01 (the closed candles) forwarded, got %v", seen)
	}
	if seen["2026-08-08T12:02:00Z"] {
		t.Errorf("in-progress 12:02 candle must not be forwarded yet, got %v", seen)
	}
}

// TestBroker_Dedup_KeysByResolution is the regression test for the
// live-blocking bug: two notifications on the same order book at
// different resolutions share a start_timestamp, but each
// resolution's candle must still be forwarded independently.
func TestBroker_Dedup_KeysByResolution(t *testing.T) {
	ob := "OB-1"
	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub1 := b.Subscribe(ob, "1m", nil)
	sub5 := b.Subscribe(ob, "5m", nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}
	push("1m", map[string][]map[string]string{ob: {
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:00:00Z", "close": "100"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "101"},
	}})
	push("5m", map[string][]map[string]string{ob: {
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:00:00Z", "close": "200"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "201"},
	}})

	got1 := false
	got5 := false
	deadline := time.After(1500 * time.Millisecond)
	for {
		select {
		case f := <-sub1.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			if meta.StartTimestamp == "2026-08-08T12:00:00Z" {
				got1 = true
			}
		case f := <-sub5.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			if meta.StartTimestamp == "2026-08-08T12:00:00Z" {
				got5 = true
			}
		case <-deadline:
			t.Fatalf("1m got=%v 5m got=%v (want both true)", got1, got5)
		}
		if got1 && got5 {
			break
		}
	}
}

// TestBroker_Dedup_SkipsInProgressTicks confirms that after
// bootstrap, in-progress single-element ticks do not leak through.
func TestBroker_Dedup_SkipsInProgressTicks(t *testing.T) {
	ob, res := "OB-1", "1m"
	bootstrap := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:00:00Z", "close": "100"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "101"},
	}
	tick1 := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "102"},
	}
	tick2 := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "103"},
	}
	newWindow := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:01:00Z", "close": "104"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T12:02:00Z", "close": "105"},
	}
	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe(ob, res, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}

	push(res, map[string][]map[string]string{ob: bootstrap})
	push(res, map[string][]map[string]string{ob: tick1})
	push(res, map[string][]map[string]string{ob: tick2})
	push(res, map[string][]map[string]string{ob: newWindow})

	got12 := false
	got1201 := false
	deadline := time.After(1 * time.Second)
	for {
		select {
		case f := <-sub.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			switch meta.StartTimestamp {
			case "2026-08-08T12:00:00Z":
				got12 = true
			case "2026-08-08T12:01:00Z":
				got1201 = true
			}
		case <-deadline:
			t.Fatalf("got12=%v got1201=%v (want both true)", got12, got1201)
		}
		if got12 && got1201 {
			break
		}
	}
	select {
	case f, ok := <-sub.Chan():
		if ok {
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			t.Errorf("unexpected third frame: start=%s", meta.StartTimestamp)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// TestBroker_EmitsOnWindowOpen_FromSingleElementTests is the
// regression test for the live-deployment bug where the broker
// never emitted a 15m candle after the initial bootstrap. The
// wsplex sends a 2-element snapshot on subscribe (bootstrap) and
// then single-element in-progress ticks. The broker must emit
// the cached in-progress candle the moment the start_timestamp
// changes (a new window opened).
func TestBroker_EmitsOnWindowOpen_FromSingleElementTests(t *testing.T) {
	ob, res := "OB-1", "15m"
	bootstrap := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T09:45:00Z", "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "5000"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "101", "low": "100", "close": "100.5", "volume": "0"},
	}
	tick1 := []map[string]string{{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "101.5", "low": "100", "close": "101", "volume": "120"}}
	tick2 := []map[string]string{{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "102", "low": "100", "close": "101.5", "volume": "300"}}
	newWindow := []map[string]string{{"order_book_id": ob, "start_timestamp": "2026-08-08T10:15:00Z", "open": "101.5", "high": "101.5", "low": "101.5", "close": "101.5", "volume": "0"}}

	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe(ob, res, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}

	push(res, map[string][]map[string]string{ob: bootstrap})
	push(res, map[string][]map[string]string{ob: tick1})
	push(res, map[string][]map[string]string{ob: tick2})
	push(res, map[string][]map[string]string{ob: newWindow})

	got09 := false
	got10 := false
	deadline := time.After(1 * time.Second)
	for {
		select {
		case f := <-sub.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			switch meta.StartTimestamp {
			case "2026-08-08T09:45:00Z":
				got09 = true
			case "2026-08-08T10:00:00Z":
				var c struct {
					Close string `json:"close"`
				}
				_ = json.Unmarshal(f.Raw, &c)
				if c.Close != "101.5" {
					t.Errorf("emitted 10:00 candle should have close=101.5 (last tick value), got %s", c.Close)
				}
				got10 = true
			}
		case <-deadline:
			t.Fatalf("got09=%v got10=%v (want both true; broker failed to emit on window open)", got09, got10)
		}
		if got09 && got10 {
			break
		}
	}
	select {
	case f, ok := <-sub.Chan():
		if ok {
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			t.Errorf("unexpected third frame: start=%s", meta.StartTimestamp)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// TestBroker_EmitsOnReconnectSnapshot is the regression test for
// the reconnect edge case: the broker has a stale in-progress
// candle in its cache from before the disconnect, the wsplex
// sends a fresh snapshot on reconnect, and the snapshot's
// bootstrap must take priority over the cached candle.
func TestBroker_EmitsOnReconnectSnapshot(t *testing.T) {
	ob, res := "OB-1", "15m"
	bootstrap := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T09:45:00Z", "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "5000"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "101", "low": "100", "close": "100.5", "volume": "0"},
	}
	tick := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "101.5", "low": "100", "close": "101", "volume": "120"},
	}
	reconnectSnapshot := []map[string]string{
		{"order_book_id": ob, "start_timestamp": "2026-08-08T10:00:00Z", "open": "100.5", "high": "102", "low": "100", "close": "101.8", "volume": "5500"},
		{"order_book_id": ob, "start_timestamp": "2026-08-08T10:15:00Z", "open": "101.8", "high": "101.8", "low": "101.8", "close": "101.8", "volume": "0"},
	}
	var (
		mu       sync.Mutex
		c        *websocket.Conn
		ready    = make(chan struct{})
		readyOne sync.Once
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		c = conn
		mu.Unlock()
		_, _, _ = conn.Read(r.Context())
		readyOne.Do(func() { close(ready) })
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, err := wsbroker.New(wsbroker.Config{URL: wsURL, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sub := b.Subscribe(ob, res, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })
	push := func(resolution string, candles map[string][]map[string]string) {
		<-ready
		mu.Lock()
		conn := c
		mu.Unlock()
		env := map[string]any{
			"kind": "notification", "path": "/charts/candles",
			"data": map[string]any{"resolution": resolution, "candles": candles},
		}
		body, _ := json.Marshal(env)
		_ = conn.Write(context.Background(), websocket.MessageText, body)
	}

	push(res, map[string][]map[string]string{ob: bootstrap})
	push(res, map[string][]map[string]string{ob: tick})
	push(res, map[string][]map[string]string{ob: reconnectSnapshot})

	got09 := false
	got10 := false
	deadline := time.After(1 * time.Second)
	for {
		select {
		case f := <-sub.Chan():
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			switch meta.StartTimestamp {
			case "2026-08-08T09:45:00Z":
				got09 = true
			case "2026-08-08T10:00:00Z":
				var c struct {
					Close string `json:"close"`
				}
				_ = json.Unmarshal(f.Raw, &c)
				if c.Close != "101.8" {
					t.Errorf("emitted 10:00 candle should have close=101.8 (from reconnect snapshot), got %s", c.Close)
				}
				got10 = true
			}
		case <-deadline:
			t.Fatalf("got09=%v got10=%v (want both true)", got09, got10)
		}
		if got09 && got10 {
			break
		}
	}
	select {
	case f, ok := <-sub.Chan():
		if ok {
			var meta struct {
				StartTimestamp string `json:"start_timestamp"`
			}
			_ = json.Unmarshal(f.Raw, &meta)
			t.Errorf("unexpected third frame: start=%s", meta.StartTimestamp)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// newTestBroker builds a broker for unit tests that exercise
// subscription registration and matching without connecting to a
// real wsplex. b.conn stays nil so Subscribe* skips the wire write.
func newTestBroker(t *testing.T) *wsbroker.Broker {
	t.Helper()
	b, err := wsbroker.New(wsbroker.Config{URL: "ws://test.local", APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

// TestBroker_Subscribe_Concurrent guards the subscription registry
// against races: 10 goroutines subscribing to the same order book +
// resolution concurrently must all receive a usable subscription and
// the broker must stay consistent (run with -race).
func TestBroker_Subscribe_Concurrent(t *testing.T) {
	b := newTestBroker(t)

	const n = 10
	subs := make([]*wsbroker.Subscription, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			subs[i] = b.Subscribe("OB-1", "1m", []string{"candle"})
		}(i)
	}
	wg.Wait()

	chans := make(map[<-chan wsbroker.Frame]bool)
	for i, sub := range subs {
		if sub == nil {
			t.Fatalf("goroutine %d: Subscribe returned nil", i)
		}
		if chans[sub.Chan()] {
			t.Errorf("goroutine %d: subscription shares a channel with another subscriber", i)
		}
		chans[sub.Chan()] = true
	}

	// Teardown: unsubscribing every concurrent subscription must close
	// its channel without deadlocking or panicking.
	for _, sub := range subs {
		b.Unsubscribe(sub)
		select {
		case _, open := <-sub.Chan():
			if open {
				t.Error("channel still open after Unsubscribe")
			}
		default:
			t.Error("Unsubscribe did not close the channel")
		}
	}
}

// TestBroker_ReconnectResendsAllSubscriptionTypes guards the
// reconnect path: after the websocket drops, the broker must re-send
// the candle, trade, AND price subscriptions on the new connection.
func TestBroker_ReconnectResendsAllSubscriptionTypes(t *testing.T) {
	var mu sync.Mutex
	// paths seen per connection (connection index -> set of paths).
	perConn := make([]map[string]bool, 0, 2)
	connCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		idx := connCount
		connCount++
		perConn = append(perConn, map[string]bool{})
		mu.Unlock()
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "test done") }()
		for {
			_, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			var env struct {
				Path string `json:"path"`
			}
			_ = json.Unmarshal(data, &env)
			mu.Lock()
			perConn[idx][env.Path] = true
			read := len(perConn[idx])
			mu.Unlock()
			if idx == 0 && read >= 3 {
				// Drop the first connection to force a reconnect.
				_ = c.Close(websocket.StatusGoingAway, "test drop")
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	b, err := wsbroker.New(wsbroker.Config{
		URL:              "ws" + strings.TrimPrefix(srv.URL, "http"),
		APIKey:           "test-key",
		ReconnectBackoff: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b.Subscribe("OB-1", "1m", []string{"candle"})
	b.SubscribeTrades("OB-1")
	b.SubscribePrices("asset-1")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()
	t.Cleanup(func() { _ = b.Stop() })

	deadline := time.After(4 * time.Second)
	for {
		mu.Lock()
		conns, secondReady := connCount, len(perConn) > 1 && len(perConn[1]) >= 3
		mu.Unlock()
		if conns >= 2 && secondReady {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			got := perConn
			mu.Unlock()
			t.Fatalf("reconnect did not re-send all subscriptions; per-connection paths: %v", got)
		case <-time.After(20 * time.Millisecond):
		}
	}

	mu.Lock()
	second := perConn[1]
	mu.Unlock()
	for _, path := range []string{"/charts/candles", "/trades", "/prices"} {
		if !second[path] {
			t.Errorf("second connection missing re-sent subscription for %s (got %v)", path, second)
		}
	}
}

// newTestBrokerWithURL builds a broker pointed at a custom wsplex URL
// (used by the httptest-based integration tests).
func newTestBrokerWithURL(t *testing.T, url string) *wsbroker.Broker {
	t.Helper()
	b, err := wsbroker.New(wsbroker.Config{URL: url, APIKey: "test-key"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

func TestHandleTradeNotification_RoutesToTradeSubscriber(t *testing.T) {
	// Spin up a fake wsplex server that pushes a /trades
	// notification; assert the subscriber sees a Frame with
	// Type="trade" and matching OrderBookID.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		// Read the subscribe envelope (ignored).
		_, _, _ = c.Read(r.Context())
		// Push a notification.
		env := map[string]any{
			"kind": "notification", "path": "/trades",
			"data": map[string]any{
				"trades": []map[string]any{
					{"transaction_id": "tx-1", "order_book_id": "OB-1", "side": "BUY"},
				},
			},
		}
		body, _ := json.Marshal(env)
		_ = c.Write(r.Context(), websocket.MessageText, body)
		time.Sleep(50 * time.Millisecond) // give the broker time to read
		_ = c.Close(websocket.StatusNormalClosure, "")
	}))
	t.Cleanup(func() { srv.Close() })

	b := newTestBrokerWithURL(t, "ws"+strings.TrimPrefix(srv.URL, "http"))
	sub := b.SubscribeTrades("OB-1")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	select {
	case f := <-sub.Chan():
		if f.Type != "trade" {
			t.Errorf("f.Type = %q, want trade", f.Type)
		}
		if f.OrderBookID != "OB-1" {
			t.Errorf("f.OrderBookID = %q", f.OrderBookID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trade frame")
	}
}

func TestHandlePriceNotification_RoutesToPriceSubscriber(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, _, _ = c.Read(r.Context())
		env := map[string]any{
			"kind": "notification", "path": "/prices",
			"data": map[string]any{
				"prices": map[string]any{
					"asset-1": map[string]any{"asset_id": "asset-1", "price": "100"},
				},
			},
		}
		body, _ := json.Marshal(env)
		_ = c.Write(r.Context(), websocket.MessageText, body)
		time.Sleep(50 * time.Millisecond)
		_ = c.Close(websocket.StatusNormalClosure, "")
	}))
	t.Cleanup(func() { srv.Close() })

	b := newTestBrokerWithURL(t, "ws"+strings.TrimPrefix(srv.URL, "http"))
	sub := b.SubscribePrices("asset-1")
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	go func() { _ = b.Start(ctx) }()

	select {
	case f := <-sub.Chan():
		if f.Type != "price" {
			t.Errorf("f.Type = %q", f.Type)
		}
		if f.AssetID != "asset-1" {
			t.Errorf("f.AssetID = %q", f.AssetID)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for price frame")
	}
}
