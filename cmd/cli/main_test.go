package main

import (
	"bytes"
	"strings"
	"testing"

	"mini-k8ts/internal/models"

	"github.com/spf13/cobra"
)

func TestCompletionCommandIncludesZsh(t *testing.T) {
	rootCmd := &cobra.Command{Use: "m8s"}
	rootCmd.AddCommand(completionCmd(rootCmd))

	buf := &bytes.Buffer{}
	rootCmd.SetOut(buf)
	rootCmd.SetArgs([]string{"completion", "zsh"})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("expected completion command to succeed, got error: %v", err)
	}

	if !strings.Contains(buf.String(), "#compdef m8s") {
		t.Fatalf("expected zsh completion output, got %q", buf.String())
	}
}

func TestFilterCompletionsMatchesPrefixAndDeduplicates(t *testing.T) {
	items, err := filterCompletions("ta", func() ([]string, error) {
		return []string{"task-1", "task-2", "node-1", "task-1"}, nil
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if items[0] != "task-1" || items[1] != "task-2" {
		t.Fatalf("unexpected completion results: %#v", items)
	}
}

func TestSortedKeysReturnsStableOrder(t *testing.T) {
	keys := sortedKeys(map[string]string{
		"beta":  "2",
		"alpha": "1",
	})
	if len(keys) != 2 || keys[0] != "alpha" || keys[1] != "beta" {
		t.Fatalf("unexpected sorted keys: %#v", keys)
	}
}

func TestParseSecretRefs(t *testing.T) {
	refs, err := parseSecretRefs([]string{"db-creds:password:DB_PASSWORD"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(refs) != 1 || refs[0].Name != "db-creds" || refs[0].EnvVar != "DB_PASSWORD" {
		t.Fatalf("unexpected refs: %#v", refs)
	}
}

func TestParseVolumeMounts(t *testing.T) {
	volumes, err := parseVolumeMounts([]string{"/tmp/data:/data:ro"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(volumes) != 1 || volumes[0].Source != "/tmp/data" || volumes[0].Target != "/data" || !volumes[0].ReadOnly {
		t.Fatalf("unexpected volumes: %#v", volumes)
	}
}

func TestTaskSummaryDetailPrefersStatusDetail(t *testing.T) {
	task := models.Task{StatusDetail: "Waiting to retry in 5s.", LastError: "older error"}
	if got := taskSummaryDetail(task); got != "Waiting to retry in 5s." {
		t.Fatalf("expected status detail, got %q", got)
	}
}

func TestTruncateTextAddsEllipsis(t *testing.T) {
	if got := truncateText("abcdefghijklmnopqrstuvwxyz", 10); got != "abcdefg..." {
		t.Fatalf("unexpected truncated text: %q", got)
	}
}
