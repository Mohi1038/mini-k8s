package logger

import "context"

type contextKey string

const requestIDContextKey contextKey = "request_id"

func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	ctx = context.WithValue(ctx, requestIDContextKey, requestID)
	ctx = context.WithValue(ctx, "request_id", requestID)
	return ctx
}

func RequestIDFromContext(ctx context.Context) string {
	if value, ok := ctx.Value(requestIDContextKey).(string); ok && value != "" {
		return value
	}
	if value, ok := ctx.Value("request_id").(string); ok {
		return value
	}
	return ""
}
