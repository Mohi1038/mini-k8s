package scheduler

import (
	"context"
	"testing"
)

type stubElector struct {
	values []bool
	index  int
}

func (s *stubElector) IsLeader(_ context.Context) (bool, error) {
	if len(s.values) == 0 {
		return false, nil
	}
	value := s.values[s.index]
	if s.index < len(s.values)-1 {
		s.index++
	}
	return value, nil
}

func TestSetLeaderElector(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, DefaultStrategy{}, nil, 0, nil)
	elector := &stubElector{values: []bool{true}}
	svc.SetLeaderElector(elector)
	if svc.elector == nil {
		t.Fatal("expected leader elector to be set")
	}
}
