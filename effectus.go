// Package effectus provides durable execution backed by SQLite.
//
// The whole model is one idea: a workflow is an ordinary Go function, and
// every side effect inside it goes through Step. Step records its result
// under (workflow id, step name). Re-running the workflow replays recorded
// results instead of executing them, so a crashed workflow resumes where it
// stopped.
//
// # Guarantees
//
//   - A step that returned successfully is never executed again.
//   - A step that crashed mid-flight is executed again (at-least-once), so
//     make external effects idempotent.
//   - Only successes are recorded. A step that returns an error (after any
//     Retry attempts) fails the workflow; running it again retries from there.
//   - At most one owner runs a workflow id at a time (a lease with a
//     heartbeat). A crashed owner's lease expires and Resume can take over.
//   - Cancelling the context passed to Run is a shutdown, not a failure: the
//     workflow stays resumable.
//
// # Rules for workflow code
//
// Code between steps must be deterministic: on replay it must take the same
// path. Put time.Now, randomness, I/O and map-order-dependent logic inside
// steps. Step names must be unique within a workflow and stable across
// deploys; they are the step's identity.
package effectus

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Status is the lifecycle state of a workflow.
type Status string

const (
	// StatusRunning: owned by a live process, or crashed and awaiting Resume.
	StatusRunning Status = "running"
	// StatusFailed: a step or the workflow function returned an error.
	// Run again to retry from the failed step.
	StatusFailed Status = "failed"
	// StatusCompleted: finished; Run is a no-op.
	StatusCompleted Status = "completed"
	// StatusCanceled: stopped by Cancel; terminal.
	StatusCanceled Status = "canceled"
)

var (
	// ErrLocked: another owner currently holds the workflow's lease.
	ErrLocked = errors.New("effectus: workflow is running elsewhere")
	// ErrConflict: the id already exists with a different kind or input.
	ErrConflict = errors.New("effectus: workflow id exists with a different kind or input")
	// ErrCanceled: the workflow was canceled.
	ErrCanceled = errors.New("effectus: workflow canceled")
	// ErrLeaseLost: this process lost ownership of the workflow mid-run.
	ErrLeaseLost = errors.New("effectus: workflow lease lost")
	// ErrNotFound: no workflow with that id.
	ErrNotFound = errors.New("effectus: workflow not found")
)

const schemaVersion = 1

// Engine owns the durable log and the workflow registry.
type Engine struct {
	db       *sql.DB
	leaseTTL time.Duration

	mu    sync.Mutex
	kinds map[string]func(ctx context.Context, id string, input []byte) error
}

// EngineOption configures Open.
type EngineOption func(*Engine)

// WithLeaseTTL sets how long a crashed owner blocks others (default 10s).
func WithLeaseTTL(d time.Duration) EngineOption {
	return func(e *Engine) {
		if d > 0 {
			e.leaseTTL = d
		}
	}
}

// Open opens (or creates) the SQLite log at path. Multiple processes may
// open the same file.
func Open(path string, opts ...EngineOption) (*Engine, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		db.Close()
		return nil, err
	}
	if v > schemaVersion {
		db.Close()
		return nil, fmt.Errorf("effectus: %s has schema version %d, this library supports up to %d", path, v, schemaVersion)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS workflows (
			id          TEXT PRIMARY KEY,
			kind        TEXT NOT NULL,
			input       BLOB NOT NULL,
			status      TEXT NOT NULL,
			error       TEXT NOT NULL DEFAULT '',
			owner       TEXT NOT NULL DEFAULT '',
			lease_until INTEGER NOT NULL DEFAULT 0  -- unix ms
		);
		CREATE TABLE IF NOT EXISTS steps (
			workflow_id TEXT NOT NULL,
			name        TEXT NOT NULL,
			result      BLOB NOT NULL,
			PRIMARY KEY (workflow_id, name)
		);
		PRAGMA user_version = 1;`); err != nil {
		db.Close()
		return nil, err
	}

	e := &Engine{db: db, leaseTTL: 10 * time.Second,
		kinds: map[string]func(context.Context, string, []byte) error{}}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

// Close closes the log. Stop workflows first.
func (e *Engine) Close() error { return e.db.Close() }

// Info describes a workflow.
type Info struct {
	ID     string
	Kind   string
	Status Status
	// Error is the failure message when Status is StatusFailed.
	Error string
	// Steps is the number of recorded (completed) steps.
	Steps int
}

const infoSelect = `
	SELECT w.id, w.kind, w.status, w.error,
	       (SELECT COUNT(*) FROM steps s WHERE s.workflow_id = w.id)
	FROM workflows w`

// Status returns information about one workflow, or ErrNotFound.
func (e *Engine) Status(ctx context.Context, id string) (Info, error) {
	var i Info
	err := e.db.QueryRowContext(ctx, infoSelect+` WHERE w.id = ?`, id).
		Scan(&i.ID, &i.Kind, &i.Status, &i.Error, &i.Steps)
	if errors.Is(err, sql.ErrNoRows) {
		return Info{}, ErrNotFound
	}
	return i, err
}

// List returns workflows, optionally filtered to the given statuses,
// ordered by id.
func (e *Engine) List(ctx context.Context, statuses ...Status) ([]Info, error) {
	q, args := infoSelect, []any{}
	if len(statuses) > 0 {
		q += ` WHERE w.status IN (?` + repeat(",?", len(statuses)-1) + `)`
		for _, s := range statuses {
			args = append(args, string(s))
		}
	}
	rows, err := e.db.QueryContext(ctx, q+` ORDER BY w.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Info
	for rows.Next() {
		var i Info
		if err := rows.Scan(&i.ID, &i.Kind, &i.Status, &i.Error, &i.Steps); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

func repeat(s string, n int) string {
	out := ""
	for range n {
		out += s
	}
	return out
}

// Cancel stops a workflow permanently. A live owner notices within a
// heartbeat, its context is cancelled and its Run returns ErrCanceled.
// Canceling a completed or already canceled workflow is a no-op.
func (e *Engine) Cancel(ctx context.Context, id string) error {
	res, err := e.db.ExecContext(ctx,
		`UPDATE workflows SET status = 'canceled', lease_until = 0
		 WHERE id = ? AND status IN ('running', 'failed')`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		_, err := e.Status(ctx, id)
		return err
	}
	return nil
}

// Delete removes a workflow and its recorded steps. Running workflows
// cannot be deleted: Cancel them first.
func (e *Engine) Delete(ctx context.Context, id string) error {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM workflows WHERE id = ? AND status != 'running'`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var s string
		err := tx.QueryRowContext(ctx, `SELECT status FROM workflows WHERE id = ?`, id).Scan(&s)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("effectus: cannot delete running workflow %q; cancel it first", id)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM steps WHERE workflow_id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}
