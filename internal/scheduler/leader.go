package scheduler

import (
	"context"
	"database/sql"
	"time"
)

type LeaderElector interface {
	IsLeader(context.Context) (bool, error)
}

type PostgresLeaderElector struct {
	db            *sql.DB
	leaseName     string
	holderID      string
	leaseDuration time.Duration
}

func NewPostgresLeaderElector(db *sql.DB, leaseName string, holderID string, leaseDuration time.Duration) *PostgresLeaderElector {
	if leaseName == "" {
		leaseName = "scheduler-control-plane"
	}
	if holderID == "" {
		holderID = "scheduler"
	}
	if leaseDuration <= 0 {
		leaseDuration = 10 * time.Second
	}
	return &PostgresLeaderElector{
		db:            db,
		leaseName:     leaseName,
		holderID:      holderID,
		leaseDuration: leaseDuration,
	}
}

func (e *PostgresLeaderElector) IsLeader(ctx context.Context) (bool, error) {
	if e == nil || e.db == nil {
		return false, nil
	}

	// Seed the lease row if it does not exist yet.
	_, err := e.db.ExecContext(ctx, `
		INSERT INTO scheduler_leases (name, holder, renew_time)
		VALUES ($1, '', TIMESTAMPTZ 'epoch')
		ON CONFLICT (name) DO NOTHING
	`, e.leaseName)
	if err != nil {
		return false, err
	}

	result, err := e.db.ExecContext(ctx, `
		UPDATE scheduler_leases
		SET holder = $2, renew_time = NOW()
		WHERE name = $1
		  AND (holder = $2 OR renew_time < NOW() - ($3 * INTERVAL '1 second'))
	`, e.leaseName, e.holderID, e.leaseDuration.Seconds())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}
