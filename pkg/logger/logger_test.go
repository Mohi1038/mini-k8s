package logger

import (
	"bytes"
	"strings"
	"testing"
)

func TestLoggerLevels(t *testing.T) {
	buffer := &bytes.Buffer{}
	log := New("debug", buffer)

	log.Debug("debug message", "component", "scheduler")
	output := buffer.String()
	if !strings.Contains(output, "debug message") {
		t.Fatalf("expected debug message in log output, got: %s", output)
	}
	if !strings.Contains(output, "\"component\":\"scheduler\"") {
		t.Fatalf("expected structured key in log output, got: %s", output)
	}
}
