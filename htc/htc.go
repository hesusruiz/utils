// Package htc provides a lightweight, debuggable HTTP client wrapper scoped to a
// single downstream service. It adds:
//   - Client-side post-completion rate limiting (minimum time gap after a call completes)
//   - Server-side rate limit handling (honoring Retry-After headers)
//   - Exponential backoff retries with jitter for transient errors (network errors, 429, 502, 503, 504)
//   - Fluent request construction (path parameters, query parameters, headers, JSON/form bodies)
//   - Resty-style Response inspection and JSON/Map/List decoding helpers
package htc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hesusruiz/utils/errl"
)

const (
	DefaultTimeout             = 10 * time.Second
	DefaultMaxRetries          = 3
	DefaultInitialWait         = 200 * time.Millisecond
	DefaultMaxWait             = 5 * time.Second
	DefaultMinInterval         = 100 * time.Millisecond
	DefaultMaxResponseBodySize = 10 * 1024 * 1024 // 10 MB
)

// Config holds the configuration for a Client talking to a downstream service.
// Zero values are replaced with sensible defaults; use DisableRetries or DisableRateLimit
// (or negative values for MaxRetries / MinInterval) to turn those features off.
type Config struct {
	// Timeout is the maximum duration for a single HTTP attempt (default: 10s).
	Timeout time.Duration

	// Headers are default HTTP headers sent with every request on this Client.
	Headers map[string]string

	// BearerToken is a static Bearer token sent in the Authorization header.
	BearerToken string

	// TokenProvider, if set, is called before each attempt to obtain a fresh Bearer token.
	// A per-request BearerToken overrides both TokenProvider and Config.BearerToken.
	TokenProvider func(ctx context.Context) (string, error)

	// MaxRetries is the maximum number of retries after the initial attempt (default: 3).
	// Set DisableRetries: true (or MaxRetries < 0) to disable retries.
	MaxRetries     int
	DisableRetries bool

	// InitialWait is the base backoff wait before the first retry (default: 200ms).
	InitialWait time.Duration

	// MaxWait is the maximum backoff wait between retries (default: 5s).
	// Note: if the server sends a Retry-After header larger than MaxWait, Retry-After is honored.
	MaxWait time.Duration

	// MinInterval is the minimum time that must elapse after a call completes
	// before the next call is sent to the server (default: 100ms).
	// Set DisableRateLimit: true (or MinInterval < 0) to disable client-side spacing.
	MinInterval      time.Duration
	DisableRateLimit bool

	// MaxResponseBodySize caps how many bytes are read from a response body (default: 10MB).
	MaxResponseBodySize int64
}

// Client is a lightweight HTTP client scoped to a single downstream service.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	baseURL    string
	cfg        Config
	httpClient *http.Client

	// gate serializes outbound calls and protects nextAllowedAt so that the server
	// never receives a new request until at least MinInterval after the previous call completed.
	gate          chan struct{}
	nextAllowedAt time.Time
}

// New creates a new Client for the given downstream baseURL.
// If cfg is nil, default settings are used.
func New(baseURL string, cfg *Config) *Client {
	var c Config
	if cfg != nil {
		c = *cfg
	}

	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxRetries < 0 {
		c.DisableRetries = true
		c.MaxRetries = 0
	} else if c.MaxRetries == 0 && !c.DisableRetries {
		c.MaxRetries = DefaultMaxRetries
	}
	if c.InitialWait <= 0 {
		c.InitialWait = DefaultInitialWait
	}
	if c.MaxWait <= 0 {
		c.MaxWait = DefaultMaxWait
	}
	if c.MinInterval < 0 {
		c.DisableRateLimit = true
		c.MinInterval = 0
	} else if c.MinInterval == 0 && !c.DisableRateLimit {
		c.MinInterval = DefaultMinInterval
	}
	if c.MaxResponseBodySize <= 0 {
		c.MaxResponseBodySize = DefaultMaxResponseBodySize
	}

	// Copy default headers map so external mutations do not affect the client.
	if len(c.Headers) > 0 {
		hdrCopy := make(map[string]string, len(c.Headers))
		maps.Copy(hdrCopy, c.Headers)
		c.Headers = hdrCopy
	}

	gate := make(chan struct{}, 1)
	gate <- struct{}{}

	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		cfg:     c,
		httpClient: &http.Client{
			Timeout: c.Timeout,
		},
		gate: gate,
	}
}

// R creates a new fluent Request bound to the given context.
func (c *Client) R(ctx context.Context) *Request {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Request{
		client:      c,
		ctx:         ctx,
		pathParams:  make(map[string]string),
		queryParams: make(url.Values),
		headers:     make(map[string]string),
	}
}

// Get sends a GET request to path.
func (c *Client) Get(ctx context.Context, path string) (*Response, error) {
	return c.R(ctx).Get(path)
}

// Post sends a POST request with the given body to path.
func (c *Client) Post(ctx context.Context, path string, body any) (*Response, error) {
	return c.R(ctx).Body(body).Post(path)
}

// Put sends a PUT request with the given body to path.
func (c *Client) Put(ctx context.Context, path string, body any) (*Response, error) {
	return c.R(ctx).Body(body).Put(path)
}

// Patch sends a PATCH request with the given body to path.
func (c *Client) Patch(ctx context.Context, path string, body any) (*Response, error) {
	return c.R(ctx).Body(body).Patch(path)
}

// Delete sends a DELETE request to path.
func (c *Client) Delete(ctx context.Context, path string) (*Response, error) {
	return c.R(ctx).Delete(path)
}

type retryOverride int

const (
	retryDefault retryOverride = iota
	retryAlways
	retryNever
)

// Request builds an individual HTTP call with optional path parameters, query parameters,
// headers, body, and retry overrides.
type Request struct {
	client      *Client
	ctx         context.Context
	pathParams  map[string]string
	queryParams url.Values
	headers     map[string]string
	bearerToken string
	body        any
	retryMode   retryOverride
}

// PathParam sets a single path parameter replacing `{key}` in the request path.
func (r *Request) PathParam(key, value string) *Request {
	r.pathParams[key] = value
	return r
}

// PathParams sets multiple path parameters replacing `{key}` placeholders in the request path.
func (r *Request) PathParams(params map[string]string) *Request {
	maps.Copy(r.pathParams, params)
	return r
}

// QueryParam adds or replaces a single query parameter.
func (r *Request) QueryParam(key, value string) *Request {
	r.queryParams.Set(key, value)
	return r
}

// QueryParams merges the given url.Values into the request's query parameters.
func (r *Request) QueryParams(params url.Values) *Request {
	for k, vals := range params {
		for _, v := range vals {
			r.queryParams.Add(k, v)
		}
	}
	return r
}

// Header sets a custom header for this request.
func (r *Request) Header(key, value string) *Request {
	r.headers[key] = value
	return r
}

// Headers sets multiple custom headers for this request.
func (r *Request) Headers(headers map[string]string) *Request {
	maps.Copy(r.headers, headers)
	return r
}

// BearerToken sets the Authorization Bearer token for this specific request.
func (r *Request) BearerToken(token string) *Request {
	r.bearerToken = token
	return r
}

// Body sets the request payload.
//   - nil: empty body
//   - []byte or string: sent as raw bytes
//   - url.Values: encoded as application/x-www-form-urlencoded
//   - any other type (struct, map, slice): JSON-marshaled with Content-Type: application/json
func (r *Request) Body(body any) *Request {
	r.body = body
	return r
}

// Form sets a url.Values payload to be sent as application/x-www-form-urlencoded.
func (r *Request) Form(data url.Values) *Request {
	r.body = data
	return r
}

// Retry forces retries to be enabled for this request even if the HTTP method is non-idempotent (e.g., POST, PATCH).
func (r *Request) Retry() *Request {
	r.retryMode = retryAlways
	return r
}

// NoRetry disables retries for this specific request.
func (r *Request) NoRetry() *Request {
	r.retryMode = retryNever
	return r
}

// Get executes the request as HTTP GET.
func (r *Request) Get(path string) (*Response, error) {
	return r.client.execute(r, http.MethodGet, path)
}

// Post executes the request as HTTP POST.
func (r *Request) Post(path string) (*Response, error) {
	return r.client.execute(r, http.MethodPost, path)
}

// Put executes the request as HTTP PUT.
func (r *Request) Put(path string) (*Response, error) {
	return r.client.execute(r, http.MethodPut, path)
}

// Patch executes the request as HTTP PATCH.
func (r *Request) Patch(path string) (*Response, error) {
	return r.client.execute(r, http.MethodPatch, path)
}

// Delete executes the request as HTTP DELETE.
func (r *Request) Delete(path string) (*Response, error) {
	return r.client.execute(r, http.MethodDelete, path)
}

// Response wraps the HTTP response from the downstream service.
// Non-2xx status codes do NOT cause Request methods to return an error; instead,
// inspect the status using IsSuccess, IsError, IsCallerError, IsServerError, or StatusCode.
type Response struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
	Attempts   int
	Duration   time.Duration
}

// IsSuccess reports whether the HTTP status code is in the 2xx range (200-299).
func (r *Response) IsSuccess() bool {
	return r != nil && r.StatusCode >= 200 && r.StatusCode <= 299
}

// IsError reports whether the HTTP status code is 400 or above.
func (r *Response) IsError() bool {
	return r != nil && r.StatusCode >= 400
}

// IsCallerError reports whether the HTTP status code is in the 4xx range (400-499).
func (r *Response) IsCallerError() bool {
	return r != nil && r.StatusCode >= 400 && r.StatusCode <= 499
}

// IsServerError reports whether the HTTP status code is in the 5xx range (500-599).
func (r *Response) IsServerError() bool {
	return r != nil && r.StatusCode >= 500 && r.StatusCode <= 599
}

// String returns the response body as a string.
func (r *Response) String() string {
	if r == nil {
		return ""
	}
	return string(r.Body)
}

// JSON unmarshals the response body into dest.
func (r *Response) JSON(dest any) error {
	if r == nil {
		return errl.Errorf("response is nil")
	}
	if len(r.Body) == 0 {
		return errl.Errorf("response body is empty (status %d)", r.StatusCode)
	}
	if err := json.Unmarshal(r.Body, dest); err != nil {
		return errl.Errorf("error unmarshaling JSON response (status %d): %w", r.StatusCode, err)
	}
	return nil
}

// Map unmarshals the JSON response body into a map[string]any.
func (r *Response) Map() (map[string]any, error) {
	var m map[string]any
	if err := r.JSON(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// List unmarshals the JSON response body into a slice of maps ([]map[string]any).
func (r *Response) List() ([]map[string]any, error) {
	var list []map[string]any
	if err := r.JSON(&list); err != nil {
		return nil, err
	}
	return list, nil
}

// execute runs the full lifecycle of a request: URL building, body serialization,
// rate limiting, HTTP execution, and retry backoff.
func (c *Client) execute(r *Request, method, path string) (*Response, error) {
	startTime := time.Now()

	fullURL, err := c.BuildURL(path, r.pathParams, r.queryParams)
	if err != nil {
		return nil, err
	}

	bodyBytes, contentType, err := prepareBody(r.body)
	if err != nil {
		return nil, err
	}

	maxRetries := c.maxRetriesFor(method, r.retryMode)

	var lastResp *Response
	var lastErr error
	attemptsMade := 0

	for attempt := 1; attempt <= maxRetries+1; attempt++ {
		attemptsMade = attempt
		resp, retryAfter, err := c.doSingleAttempt(r.ctx, method, fullURL, bodyBytes, contentType, r)
		lastResp = resp
		lastErr = err

		// Stop immediately if this was our last allowed attempt.
		if attempt > maxRetries {
			break
		}

		// Never retry if the caller's context itself was canceled or expired.
		if r.ctx.Err() != nil {
			return nil, errl.Errorf("htc %s %s canceled: %w", method, path, r.ctx.Err())
		}

		if !shouldRetry(resp, err) {
			break
		}

		wait := c.computeBackoff(attempt, retryAfter)
		slog.Warn("htc retrying request",
			slog.String("method", method),
			slog.String("path", path),
			slog.Int("attempt", attempt),
			slog.Duration("wait", wait),
			slog.Any("err", err),
			slog.Int("status", statusCodeOf(resp)),
		)

		if err := sleepWithContext(r.ctx, wait); err != nil {
			return nil, errl.Errorf("htc %s %s canceled during retry backoff: %w", method, path, err)
		}
	}

	if lastErr != nil {
		return nil, errl.Errorf("htc %s %s failed after %d attempt(s): %w", method, path, attemptsMade, lastErr)
	}

	lastResp.Attempts = attemptsMade
	lastResp.Duration = time.Since(startTime)
	return lastResp, nil
}

// doSingleAttempt acquires the rate-limiting gate, waits until nextAllowedAt, performs one
// HTTP transaction, reads the body, updates nextAllowedAt based on completion time and Retry-After,
// and releases the gate.
func (c *Client) doSingleAttempt(
	ctx context.Context,
	method string,
	fullURL string,
	bodyBytes []byte,
	contentType string,
	r *Request,
) (*Response, time.Duration, error) {
	// 1. Acquire the single-flight gate so calls are serialized and properly spaced.
	select {
	case <-c.gate:
	case <-ctx.Done():
		return nil, 0, errl.Error(ctx.Err())
	}

	// 2. Wait until the post-completion cooldown (or server Retry-After) from the previous call has elapsed.
	if wait := time.Until(c.nextAllowedAt); wait > 0 {
		if err := sleepWithContext(ctx, wait); err != nil {
			c.gate <- struct{}{}
			return nil, 0, errl.Error(err)
		}
	}

	// Ensure we always update nextAllowedAt on completion and release the gate.
	var retryAfter time.Duration
	defer func() {
		completedAt := time.Now()
		if !c.cfg.DisableRateLimit && c.cfg.MinInterval > 0 {
			if minNext := completedAt.Add(c.cfg.MinInterval); minNext.After(c.nextAllowedAt) {
				c.nextAllowedAt = minNext
			}
		}
		if retryAfter > 0 {
			if raNext := completedAt.Add(retryAfter); raNext.After(c.nextAllowedAt) {
				c.nextAllowedAt = raNext
			}
		}
		c.gate <- struct{}{}
	}()

	// 3. Resolve Bearer token (per-request > TokenProvider > static Config.BearerToken).
	token := r.bearerToken
	if token == "" && c.cfg.TokenProvider != nil {
		var err error
		token, err = c.cfg.TokenProvider(ctx)
		if err != nil {
			return nil, 0, errl.Errorf("token provider failed: %w", err)
		}
	}
	if token == "" {
		token = c.cfg.BearerToken
	}

	// 4. Build the standard net/http request.
	var bodyReader io.Reader
	if bodyBytes != nil {
		bodyReader = bytes.NewReader(bodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return nil, 0, errl.Errorf("error creating HTTP request: %w", err)
	}

	// Apply client-level default headers.
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}
	// Apply inferred Content-Type if not already set by default headers or request headers.
	if contentType != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}
	// Apply per-request headers (can override Content-Type or default headers).
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	// Apply Authorization Bearer token if not explicitly set in headers.
	if token != "" && req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	// 5. Execute the HTTP call.
	httpResp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, errl.Error(err)
	}
	defer httpResp.Body.Close()

	retryAfter = parseRetryAfter(httpResp.Header.Get("Retry-After"), time.Now())

	// 6. Read the response body up to MaxResponseBodySize + 1 to detect oversized payloads.
	limitedReader := io.LimitReader(httpResp.Body, c.cfg.MaxResponseBodySize+1)
	respBytes, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, retryAfter, errl.Errorf("error reading response body: %w", err)
	}
	if int64(len(respBytes)) > c.cfg.MaxResponseBodySize {
		return nil, retryAfter, errl.Errorf("response body exceeds limit of %d bytes", c.cfg.MaxResponseBodySize)
	}

	resp := &Response{
		StatusCode: httpResp.StatusCode,
		Status:     httpResp.Status,
		Headers:    httpResp.Header.Clone(),
		Body:       respBytes,
		Attempts:   1,
	}

	return resp, retryAfter, nil
}

// BuildURL resolves `{param}` placeholders in path, joins with baseURL, and merges query parameters.
func (c *Client) BuildURL(path string, pathParams map[string]string, queryParams url.Values) (string, error) {
	resolvedPath := path
	for k, v := range pathParams {
		placeholder := "{" + k + "}"
		resolvedPath = strings.ReplaceAll(resolvedPath, placeholder, url.PathEscape(v))
	}

	var rawURL string
	if strings.HasPrefix(resolvedPath, "https://") || strings.HasPrefix(resolvedPath, "http://") {
		rawURL = resolvedPath
	} else if c.baseURL == "" {
		return "", errl.Errorf("baseURL is empty and path %q is not an absolute URL", path)
	} else if resolvedPath == "" {
		rawURL = c.baseURL
	} else {
		rawURL = c.baseURL + "/" + strings.TrimLeft(resolvedPath, "/")
	}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", errl.Errorf("invalid URL %q: %w", rawURL, err)
	}

	if len(queryParams) > 0 {
		q := parsedURL.Query()
		for k, vals := range queryParams {
			for _, v := range vals {
				q.Add(k, v)
			}
		}
		parsedURL.RawQuery = q.Encode()
	}

	return parsedURL.String(), nil
}

// prepareBody serializes the request body into a reusable byte slice and determines its Content-Type.
func prepareBody(body any) ([]byte, string, error) {
	if body == nil {
		return nil, "", nil
	}
	switch v := body.(type) {
	case []byte:
		return v, "", nil
	case string:
		return []byte(v), "", nil
	case url.Values:
		return []byte(v.Encode()), "application/x-www-form-urlencoded", nil
	case io.Reader:
		b, err := io.ReadAll(v)
		if err != nil {
			return nil, "", errl.Errorf("error reading request body: %w", err)
		}
		return b, "", nil
	default:
		b, err := json.Marshal(body)
		if err != nil {
			return nil, "", errl.Errorf("error marshaling request body to JSON: %w", err)
		}
		return b, "application/json", nil
	}
}

func (c *Client) maxRetriesFor(method string, mode retryOverride) int {
	if mode == retryNever || c.cfg.DisableRetries || c.cfg.MaxRetries <= 0 {
		return 0
	}
	if mode == retryAlways || isIdempotent(method) {
		return c.cfg.MaxRetries
	}
	return 0
}

func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions:
		return true
	default:
		return false
	}
}

func shouldRetry(resp *Response, err error) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return true
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// computeBackoff calculates exponential backoff with jitter in [0.75 * exp, 1.0 * exp],
// honoring server-provided retryAfter if it is larger.
func (c *Client) computeBackoff(attempt int, retryAfter time.Duration) time.Duration {
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	backoff := c.cfg.InitialWait * time.Duration(1<<shift)
	if backoff > c.cfg.MaxWait || backoff <= 0 {
		backoff = c.cfg.MaxWait
	}

	// Apply jitter in [75%, 100%] of the computed backoff.
	jitterFactor := 0.75 + rand.Float64()*0.25
	wait := time.Duration(float64(backoff) * jitterFactor)

	if retryAfter > wait {
		wait = retryAfter
	}
	return wait
}

// parseRetryAfter parses an HTTP Retry-After header value (either delay-seconds or HTTP-date).
func parseRetryAfter(val string, now time.Time) time.Duration {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	if secs, err := strconv.Atoi(val); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(val); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func statusCodeOf(resp *Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// Errorf is a small helper for formatting errors if needed by callers.
func (r *Response) Errorf(format string, a ...any) error {
	return errl.Errorf(format, a...)
}

// StatusError returns a descriptive error if the response is not 2xx, or nil if IsSuccess() is true.
func (r *Response) StatusError() error {
	if r == nil {
		return errl.Errorf("response is nil")
	}
	if r.IsSuccess() {
		return nil
	}
	return errl.Errorf("HTTP %d (%s): %s", r.StatusCode, r.Status, string(r.Body))
}
