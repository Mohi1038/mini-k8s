package api

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"mini-k8ts/pkg/logger"
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func (rr *responseRecorder) WriteHeader(statusCode int) {
	rr.statusCode = statusCode
	rr.ResponseWriter.WriteHeader(statusCode)
}

func (rr *responseRecorder) Write(body []byte) (int, error) {
	if rr.statusCode == 0 {
		rr.statusCode = http.StatusOK
	}
	written, err := rr.ResponseWriter.Write(body)
	rr.bytes += written
	return written, err
}

func requestLoggerMiddleware(log *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = time.Now().UTC().Format("20060102150405.000000000")
		}

		ctx := logger.ContextWithRequestID(r.Context(), requestID)
		r = r.WithContext(ctx)

		recorder := &responseRecorder{ResponseWriter: w}

		start := time.Now()
		next.ServeHTTP(recorder, r)
		log.With("request_id", requestID).Info(
			"http request completed",
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.statusCode,
			"bytes", recorder.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Idempotency-Key, X-Service-Key, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type authOptions struct {
	token      string
	allowPaths map[string]struct{}
}

func authMiddleware(next http.Handler, opts authOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || opts.token == "" || isPathAllowlisted(r.URL.Path, opts.allowPaths) {
			next.ServeHTTP(w, r)
			return
		}

		if token := strings.TrimSpace(r.Header.Get("X-Service-Key")); token != "" && token == opts.token {
			next.ServeHTTP(w, r)
			return
		}
		if bearer := strings.TrimSpace(r.Header.Get("Authorization")); bearer != "" {
			parts := strings.Fields(bearer)
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") && strings.TrimSpace(parts[1]) == opts.token {
				next.ServeHTTP(w, r)
				return
			}
		}

		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("unauthorized"))
	})
}

type rateLimitOptions struct {
	rpm        int
	burst      int
	allowPaths map[string]struct{}
}

type rateLimiter struct {
	rpm        int
	capacity   float64
	refillRate float64
	mu         sync.Mutex
	buckets    map[string]*rateBucket
}

type rateBucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(rpm int, burst int) *rateLimiter {
	if burst <= 0 {
		burst = rpm
	}
	return &rateLimiter{
		rpm:        rpm,
		capacity:   float64(burst),
		refillRate: float64(rpm) / 60.0,
		buckets:    map[string]*rateBucket{},
	}
}

func (r *rateLimiter) allow(key string) (float64, time.Duration, bool) {
	if r == nil || r.rpm <= 0 {
		return 0, 0, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	bucket, ok := r.buckets[key]
	if !ok {
		bucket = &rateBucket{tokens: r.capacity, last: now}
		r.buckets[key] = bucket
	}

	elapsed := now.Sub(bucket.last).Seconds()
	bucket.tokens += elapsed * r.refillRate
	if bucket.tokens > r.capacity {
		bucket.tokens = r.capacity
	}
	bucket.last = now

	if bucket.tokens < 1 {
		retryAfter := time.Duration((1-bucket.tokens)/r.refillRate*float64(time.Second))
		return bucket.tokens, retryAfter, false
	}

	bucket.tokens -= 1
	return bucket.tokens, 0, true
}

func rateLimitMiddleware(next http.Handler, opts rateLimitOptions) http.Handler {
	limiter := newRateLimiter(opts.rpm, opts.burst)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opts.rpm <= 0 || r.Method == http.MethodOptions || isPathAllowlisted(r.URL.Path, opts.allowPaths) {
			next.ServeHTTP(w, r)
			return
		}

		key := requestKey(r)
		remaining, retryAfter, ok := limiter.allow(key)
		if !ok {
			if retryAfter > 0 {
				w.Header().Set("Retry-After", fmtDurationSeconds(retryAfter))
			}
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limit exceeded"))
			return
		}
		w.Header().Set("X-RateLimit-Remaining", formatRemaining(remaining))
		next.ServeHTTP(w, r)
	})
}

func requestKey(r *http.Request) string {
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func isPathAllowlisted(path string, allowPaths map[string]struct{}) bool {
	if len(allowPaths) == 0 {
		return false
	}
	_, ok := allowPaths[path]
	return ok
}

func formatRemaining(value float64) string {
	if value < 0 {
		value = 0
	}
	return strconv.FormatInt(int64(value), 10)
}

func fmtDurationSeconds(value time.Duration) string {
	seconds := int(value.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("%d", seconds)
}
