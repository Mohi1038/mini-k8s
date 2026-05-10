package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type Logger struct {
	base *slog.Logger
}

func New(level string, out io.Writer) *Logger {
	if out == nil {
		out = os.Stdout
	}

	handler := slog.NewJSONHandler(out, &slog.HandlerOptions{Level: parseLevel(level)})
	return &Logger{base: slog.New(handler)}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.base.Debug(msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.base.Info(msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.base.Warn(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.base.Error(msg, args...)
}

func (l *Logger) With(args ...any) *Logger {
	return &Logger{base: l.base.With(args...)}
}

func (l *Logger) WithContext(ctx context.Context) *Logger {
	requestID := RequestIDFromContext(ctx)
	if requestID == "" {
		return l
	}

	return l.With("request_id", requestID)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
