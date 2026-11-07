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

	// POST with .Retry() retries and preserves body across attempts
	postAttempts.Store(0)
	resp, err = client.R(ctx).
		Body(map[string]string{"payload": "important"}).
		Retry().
		Post("/flaky-post")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.IsSuccess() || resp.Attempts != 2 {
		t.Fatalf("expected 200 after 2 attempts for opted-in POST, got status=%d attempts=%d", resp.StatusCode, resp.Attempts)
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
