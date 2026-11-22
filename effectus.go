// Package effectus provides durable execution backed by SQLite.
//
// The whole model is one idea: a workflow is an ordinary Go function, and
// every side effect inside it goes through Step. Step records its result
// under (workflow id, step name). Re-running the workflow replays recorded
// results instead of executing them, so a crashed workflow resumes where it
// stopped.
//
// Guarantees:
//   - A step that returned successfully is never executed again.
//   - A step that crashed mid-flight is executed again (at-least-once), so
//     make external effects idempotent.
//   - Only successes are recorded. A step that returns an error fails the
//     workflow; running it again retries from that step.
package effectus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Engine owns the durable log.
type Engine struct{ db *sql.DB }

// Open opens (or creates) the SQLite log at path.
func Open(path string) (*Engine, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS workflows (
			id     TEXT PRIMARY KEY,
			status TEXT NOT NULL,  -- running | failed | completed
			error  TEXT
		);
		CREATE TABLE IF NOT EXISTS steps (
			workflow_id TEXT NOT NULL,
			name        TEXT NOT NULL,
			result      BLOB NOT NULL,
			PRIMARY KEY (workflow_id, name)
		);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Engine{db}, nil
}

func (e *Engine) Close() error { return e.db.Close() }

type run struct {
	e    *Engine
	id   string
	mu   sync.Mutex
	seen map[string]bool
}

type ctxKey struct{}

// Run executes fn as the workflow identified by id. It is idempotent: if the
// workflow already completed, Run returns nil without calling fn. Otherwise
// fn runs from the top, replaying recorded steps.
func (e *Engine) Run(ctx context.Context, id string, fn func(ctx context.Context) error) error {
	var status string
	err := e.db.QueryRowContext(ctx, `SELECT status FROM workflows WHERE id = ?`, id).Scan(&status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case status == "completed":
		return nil
	}
	if _, err := e.db.ExecContext(ctx,
		`INSERT INTO workflows (id, status) VALUES (?, 'running')
		 ON CONFLICT(id) DO UPDATE SET status = 'running', error = NULL`, id); err != nil {
		return err
	}

	r := &run{e: e, id: id, seen: map[string]bool{}}
	werr := fn(context.WithValue(ctx, ctxKey{}, r))

	// Use a fresh context so a cancelled ctx still records the outcome.
	bg := context.WithoutCancel(ctx)
	if werr != nil {
		e.db.ExecContext(bg, `UPDATE workflows SET status = 'failed', error = ? WHERE id = ?`, werr.Error(), id)
		return werr
	}
	_, err = e.db.ExecContext(bg, `UPDATE workflows SET status = 'completed' WHERE id = ?`, id)
	return err
}

// Step runs fn once, durably. On replay it returns the recorded value.
// Name must be unique within the workflow and stable across deploys; it is
// the step's identity. T must be JSON-serializable.
func Step[T any](ctx context.Context, name string, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	r, _ := ctx.Value(ctxKey{}).(*run)
	if r == nil {
		return zero, errors.New("effectus: Step called outside of a workflow")
	}
	r.mu.Lock()
	dup := r.seen[name]
	r.seen[name] = true
	r.mu.Unlock()
	if dup {
		return zero, fmt.Errorf("effectus: duplicate step name %q (loops need unique names)", name)
	}

	var raw []byte
	err := r.e.db.QueryRowContext(ctx,
		`SELECT result FROM steps WHERE workflow_id = ? AND name = ?`, r.id, name).Scan(&raw)
	if err == nil {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			return zero, fmt.Errorf("effectus: step %q: decode recorded result: %w", name, err)
		}
		return v, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return zero, err
	}

	v, err := fn(ctx)
	if err != nil {
		return zero, err
	}
	raw, err = json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("effectus: step %q: encode result: %w", name, err)
	}
	_, err = r.e.db.ExecContext(context.WithoutCancel(ctx),
		`INSERT INTO steps (workflow_id, name, result) VALUES (?, ?, ?)`, r.id, name, raw)
	return v, err
}

// Do is Step for effects with no result.
func Do(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	_, err := Step(ctx, name, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// Sleep pauses the workflow until a durable deadline. The deadline is
// recorded, so a restart sleeps only for the remaining time.
func Sleep(ctx context.Context, name string, d time.Duration) error {
	until, err := Step(ctx, name, func(context.Context) (time.Time, error) {
		return time.Now().Add(d), nil
	})
	if err != nil {
		return err
	}
	t := time.NewTimer(time.Until(until))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
