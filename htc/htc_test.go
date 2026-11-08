package htc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerbsAndPayloads(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/users/42":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":     42,
				"name":   "Alice",
				"filter": r.URL.Query().Get("filter"),
				"auth":   r.Header.Get("Authorization"),
				"custom": r.Header.Get("X-Custom"),
			})

		case r.Method == http.MethodGet && r.URL.Path == "/items":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 1, "title": "first"},
				{"id": 2, "title": "second"},
			})

		case r.Method == http.MethodPost && r.URL.Path == "/users":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"created": body["name"],
				"ct":      r.Header.Get("Content-Type"),
			})

		case r.Method == http.MethodPost && r.URL.Path == "/login":
			if err := r.ParseForm(); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"user": r.FormValue("username"),
				"ct":   r.Header.Get("Content-Type"),
			})

		case r.Method == http.MethodPut && r.URL.Path == "/users/42":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"updated":true}`))

		case r.Method == http.MethodPatch && r.URL.Path == "/users/42":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"patched":true}`))

		case r.Method == http.MethodDelete && r.URL.Path == "/users/42":
			w.WriteHeader(http.StatusNoContent)

		case r.URL.Path == "/error/404":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))

		case r.URL.Path == "/error/500":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal error"}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := New(srv.URL, &Config{
		Headers:          map[string]string{"X-Custom": "default-val"},
		BearerToken:      "secret-token",
		DisableRateLimit: true,
	})

	ctx := context.Background()

	// 1. Fluent GET with PathParam, QueryParams, and Map()
	resp, err := client.R(ctx).
		PathParam("id", "42").
		QueryParams(url.Values{"filter": {"active"}}).
		Get("/users/{id}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsSuccess() || resp.IsError() {
		t.Fatalf("expected 2xx success, got %d", resp.StatusCode)
	}
	m, err := resp.Map()
	if err != nil {
		t.Fatalf("Map() failed: %v", err)
	}
	if m["name"] != "Alice" || m["filter"] != "active" || m["auth"] != "Bearer secret-token" || m["custom"] != "default-val" {
		t.Fatalf("unexpected response map: %+v", m)
	}

	// 2. Direct Get + List()
	resp, err = client.Get(ctx, "/items")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	list, err := resp.List()
	if err != nil {
		t.Fatalf("List() failed: %v", err)
	}
	if len(list) != 2 || list[0]["title"] != "first" {
		t.Fatalf("unexpected list: %+v", list)
	}

	// 3. Direct Post with JSON struct + JSON(&dest)
	type createReq struct {
		Name string `json:"name"`
	}
	type createResp struct {
		Created string `json:"created"`
		CT      string `json:"ct"`
	}
	resp, err = client.Post(ctx, "/users", createReq{Name: "Bob"})
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated || !resp.IsSuccess() {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}
	var cResp createResp
	if err := resp.JSON(&cResp); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}
	if cResp.Created != "Bob" || cResp.CT != "application/json" {
		t.Fatalf("unexpected createResp: %+v", cResp)
	}

	// 4. Form url.Values POST
	resp, err = client.Post(ctx, "/login", url.Values{"username": {"charlie"}})
	if err != nil {
		t.Fatalf("Form Post failed: %v", err)
	}
	loginMap, err := resp.Map()
	if err != nil {
		t.Fatalf("Map() failed: %v", err)
	}
	if loginMap["user"] != "charlie" || loginMap["ct"] != "application/x-www-form-urlencoded" {
		t.Fatalf("unexpected login response: %+v", loginMap)
	}

	// 5. Put, Patch, Delete
	if resp, err = client.Put(ctx, "/users/42", map[string]any{"a": 1}); err != nil || !resp.IsSuccess() {
		t.Fatalf("Put failed: err=%v status=%v", err, resp)
	}
	if resp, err = client.Patch(ctx, "/users/42", map[string]any{"a": 2}); err != nil || !resp.IsSuccess() {
		t.Fatalf("Patch failed: err=%v status=%v", err, resp)
	}
	if resp, err = client.Delete(ctx, "/users/42"); err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Delete failed: err=%v status=%v", err, resp)
	}

	// 6. Status classification helpers (4xx and 5xx do not return error)
	resp, err = client.Get(ctx, "/error/404")
	if err != nil {
		t.Fatalf("expected nil error on 404, got %v", err)
	}
	if !resp.IsError() || !resp.IsCallerError() || resp.IsServerError() || resp.IsSuccess() {
		t.Fatalf("wrong status flags for 404: %+v", resp)
	}

	resp, err = client.Get(ctx, "/error/500")
	if err != nil {
		t.Fatalf("expected nil error on 500, got %v", err)
	}
	if !resp.IsError() || !resp.IsServerError() || resp.IsCallerError() || resp.IsSuccess() {
		t.Fatalf("wrong status flags for 500: %+v", resp)
	}
}

func TestRetriesAndIdempotency(t *testing.T) {
	var getAttempts atomic.Int32
	var postAttempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/flaky-get":
			n := getAttempts.Add(1)
			if n < 3 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusServiceUnavailable) // 503 is retryable
				_, _ = w.Write([]byte(`{"status":"busy"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))

		case "/flaky-post":
			n := postAttempts.Add(1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["payload"] != "important" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if n < 2 {
				w.WriteHeader(http.StatusTooManyRequests) // 429 is retryable when opted in
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"created"}`))
		}
	}))
	defer srv.Close()

	client := New(srv.URL, &Config{
		MaxRetries:       3,
		InitialWait:      10 * time.Millisecond,
		MaxWait:          50 * time.Millisecond,
		DisableRateLimit: true,
	})

	ctx := context.Background()

	// GET retries automatically and succeeds on 3rd attempt
	resp, err := client.Get(ctx, "/flaky-get")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsSuccess() || resp.Attempts != 3 {
		t.Fatalf("expected success after 3 attempts, got status=%d attempts=%d", resp.StatusCode, resp.Attempts)
	}

	// POST does NOT retry by default
	resp, err = client.Post(ctx, "/flaky-post", map[string]string{"payload": "important"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusTooManyRequests || resp.Attempts != 1 {
		t.Fatalf("expected 429 after 1 attempt for non-idempotent POST, got status=%d attempts=%d", resp.StatusCode, resp.Attempts)
	}

	// Client with Config.RetryPost = true retries client.Post() automatically and preserves body
	postClient := New(srv.URL, &Config{
		MaxRetries:       3,
		RetryPost:        true,
		InitialWait:      10 * time.Millisecond,
		MaxWait:          50 * time.Millisecond,
		DisableRateLimit: true,
	})
	postAttempts.Store(0)
	resp, err = postClient.Post(ctx, "/flaky-post", map[string]string{"payload": "important"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsSuccess() || resp.Attempts != 2 {
		t.Fatalf("expected 200 after 2 attempts with Config.RetryPost=true, got status=%d attempts=%d", resp.StatusCode, resp.Attempts)
	}
}

func TestPostCompletionRateLimiting(t *testing.T) {
	const minGap = 80 * time.Millisecond
	const serverWorkTime = 30 * time.Millisecond

	var mu sync.Mutex
	type callWindow struct {
		arrivedAt   time.Time
		completedAt time.Time
	}
	var windows []callWindow

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived := time.Now()
		time.Sleep(serverWorkTime)
		completed := time.Now()

		mu.Lock()
		windows = append(windows, callWindow{arrivedAt: arrived, completedAt: completed})
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	client := New(srv.URL, &Config{
		MinInterval:    minGap,
		DisableRetries: true,
	})

	// Launch 4 concurrent calls; the server must never receive call i+1
	// earlier than ~minGap after call i finished.
	const numCalls = 4
	var wg sync.WaitGroup
	wg.Add(numCalls)
	for range numCalls {
		go func() {
			defer wg.Done()
			resp, err := client.Get(context.Background(), "/work")
			if err != nil || !resp.IsSuccess() {
				t.Errorf("call failed: err=%v resp=%v", err, resp)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(windows) != numCalls {
		t.Fatalf("expected %d calls, got %d", numCalls, len(windows))
	}

	for i := 1; i < len(windows); i++ {
		gap := windows[i].arrivedAt.Sub(windows[i-1].completedAt)
		// Allow 5ms tolerance for OS timer scheduling jitter
		if gap < minGap-5*time.Millisecond {
			t.Fatalf("call %d arrived %v after call %d completed; expected at least %v",
				i, gap, i-1, minGap)
		}
	}
}

func TestTokenBucketRateLimiting(t *testing.T) {
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var mu sync.Mutex
	var arrivals []time.Time

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := inFlight.Add(1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}

		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()

		// Simulate 60ms of server work so burst calls overlap in-flight
		time.Sleep(60 * time.Millisecond)
		inFlight.Add(-1)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	// 20 requests/sec (1 token every 50ms) with a burst of 2
	client := New(srv.URL, &Config{
		RequestsPerSecond: 20,
		Burst:             2,
		DisableRetries:    true,
	})

	start := time.Now()
	const numCalls = 4
	var wg sync.WaitGroup
	wg.Add(numCalls)
	for range numCalls {
		go func() {
			defer wg.Done()
			resp, err := client.Get(context.Background(), "/burst")
			if err != nil || !resp.IsSuccess() {
				t.Errorf("call failed: err=%v resp=%v", err, resp)
			}
		}()
	}
	wg.Wait()
	totalElapsed := time.Since(start)

	// Because Burst == 2 and MinInterval == 0, the first 2 calls run concurrently in-flight,
	// call 3 starts at ~50ms, and call 4 starts at ~100ms.
	if maxInFlight.Load() < 2 {
		t.Fatalf("expected concurrent in-flight requests with Burst=2, got maxInFlight=%d", maxInFlight.Load())
	}
	if len(arrivals) != numCalls {
		t.Fatalf("expected %d arrivals, got %d", numCalls, len(arrivals))
	}
	// 4 calls with burst 2 at 20 req/s (50ms spacing) requires ~100ms before the 4th call fires
	if totalElapsed < 90*time.Millisecond {
		t.Fatalf("expected token bucket to throttle calls 3 & 4 (~100ms+), but finished in %v", totalElapsed)
	}
}

