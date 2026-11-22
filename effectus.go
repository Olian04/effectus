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
//   - Only successes are recorded. A step that returns an error (after any
//     Retry attempts) fails the workflow; running it again retries from there.
//   - At most one process runs a given workflow id at a time (a lease with a
//     heartbeat). A crashed owner's lease expires and the workflow can resume.
//
// Code between steps must be deterministic: on replay it must take the same
// path. Put time.Now, rand, I/O and map-order-dependent logic inside steps.
package effectus

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrLocked means another owner currently holds the workflow's lease.
var ErrLocked = errors.New("effectus: workflow is running elsewhere")

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
func WithLeaseTTL(d time.Duration) EngineOption { return func(e *Engine) { e.leaseTTL = d } }

// Open opens (or creates) the SQLite log at path.
func Open(path string, opts ...EngineOption) (*Engine, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS workflows (
			id          TEXT PRIMARY KEY,
			kind        TEXT NOT NULL,
			input       BLOB NOT NULL,
			status      TEXT NOT NULL,  -- running | failed | completed
			error       TEXT,
			owner       TEXT NOT NULL DEFAULT '',
			lease_until INTEGER NOT NULL DEFAULT 0  -- unix ms
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
	e := &Engine{db: db, leaseTTL: 10 * time.Second,
		kinds: map[string]func(context.Context, string, []byte) error{}}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

func (e *Engine) Close() error { return e.db.Close() }

type run struct {
	e    *Engine
	id   string
	mu   sync.Mutex
	seen map[string]bool
}

type ctxKey struct{}

// Workflow is a registered, typed workflow definition.
type Workflow[In any] struct {
	e    *Engine
	kind string
	fn   func(ctx context.Context, in In) error
}

// Register defines a workflow kind. The input is persisted with the
// workflow so that Resume can restart it after a crash. Register every kind
// before calling Resume, and keep kind names stable across deploys.
func Register[In any](e *Engine, kind string, fn func(ctx context.Context, in In) error) *Workflow[In] {
	w := &Workflow[In]{e: e, kind: kind, fn: fn}
	e.mu.Lock()
	e.kinds[kind] = func(ctx context.Context, id string, raw []byte) error {
		var in In
		if err := json.Unmarshal(raw, &in); err != nil {
			return fmt.Errorf("effectus: workflow %q: decode input: %w", id, err)
		}
		return e.exec(ctx, kind, id, raw, func(ctx context.Context) error { return fn(ctx, in) })
	}
	e.mu.Unlock()
	return w
}

// Run executes the workflow identified by id. It is idempotent: if the
// workflow already completed, Run returns nil without calling the function.
// Otherwise the function runs from the top, replaying recorded steps. If
// another owner holds the lease, Run returns ErrLocked.
func (w *Workflow[In]) Run(ctx context.Context, id string, in In) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("effectus: encode input: %w", err)
	}
	return w.e.exec(ctx, w.kind, id, raw, func(ctx context.Context) error { return w.fn(ctx, in) })
}

// Resume re-runs every workflow that was left running by a crashed owner
// (expired lease) and whose kind is registered. It blocks until they finish.
// Workflows that failed are not resumed; call Run to retry those.
func (e *Engine) Resume(ctx context.Context) error {
	rows, err := e.db.QueryContext(ctx,
		`SELECT id, kind, input FROM workflows WHERE status = 'running' AND lease_until < ?`, nowMs())
	if err != nil {
		return err
	}
	type job struct {
		id, kind string
		input    []byte
	}
	var jobs []job
	for rows.Next() {
		var j job
		if err := rows.Scan(&j.id, &j.kind, &j.input); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, j)
	}
	rows.Close()

	var (
		wg   sync.WaitGroup
		emu  sync.Mutex
		errs []error
	)
	for _, j := range jobs {
		e.mu.Lock()
		start := e.kinds[j.kind]
		e.mu.Unlock()
		if start == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := start(ctx, j.id, j.input); err != nil && !errors.Is(err, ErrLocked) {
				emu.Lock()
				errs = append(errs, fmt.Errorf("workflow %q: %w", j.id, err))
				emu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func randID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func nowMs() int64 { return time.Now().UnixMilli() }

func (e *Engine) exec(ctx context.Context, kind, id string, input []byte, fn func(ctx context.Context) error) error {
	owner := randID()
	res, err := e.db.ExecContext(ctx, `
		INSERT INTO workflows (id, kind, input, status, owner, lease_until)
		VALUES (?, ?, ?, 'running', ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = 'running', error = NULL,
			owner = excluded.owner, lease_until = excluded.lease_until
		WHERE workflows.status != 'completed' AND workflows.lease_until < ?`,
		id, kind, input, owner, nowMs()+e.leaseTTL.Milliseconds(), nowMs())
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		var status string
		if err := e.db.QueryRowContext(ctx, `SELECT status FROM workflows WHERE id = ?`, id).Scan(&status); err != nil {
			return err
		}
		if status == "completed" {
			return nil
		}
		return ErrLocked
	}

	// Heartbeat: keep the lease alive; if it is lost, stop the workflow.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(e.leaseTTL / 3)
		defer t.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-t.C:
				r, err := e.db.ExecContext(wctx,
					`UPDATE workflows SET lease_until = ? WHERE id = ? AND owner = ?`,
					nowMs()+e.leaseTTL.Milliseconds(), id, owner)
				if err == nil {
					if n, _ := r.RowsAffected(); n == 0 {
						cancel()
						return
					}
				}
			}
		}
	}()

	r := &run{e: e, id: id, seen: map[string]bool{}}
	werr := fn(context.WithValue(wctx, ctxKey{}, r))
	cancel()
	<-hbDone

	// Record the outcome even if ctx was cancelled. A crash (no outcome
	// recorded) leaves status 'running' with an expiring lease: resumable.
	bg := context.WithoutCancel(ctx)
	if werr != nil {
		e.db.ExecContext(bg, `UPDATE workflows SET status = 'failed', error = ?, lease_until = 0
			WHERE id = ? AND owner = ?`, werr.Error(), id, owner)
		return werr
	}
	_, err = e.db.ExecContext(bg, `UPDATE workflows SET status = 'completed', lease_until = 0
		WHERE id = ? AND owner = ?`, id, owner)
	return err
}

// Option configures a Step.
type Option func(*stepOpts)

type stepOpts struct {
	attempts int
	backoff  time.Duration
}

// Retry makes a step run up to attempts times in total, sleeping backoff,
// 2*backoff, 4*backoff... between attempts. Retries happen in-process; only
// the final success is recorded.
func Retry(attempts int, backoff time.Duration) Option {
	return func(o *stepOpts) { o.attempts, o.backoff = attempts, backoff }
}

// Step runs fn once, durably. On replay it returns the recorded value.
// Name must be unique within the workflow and stable across deploys; it is
// the step's identity. T must be JSON-serializable.
func Step[T any](ctx context.Context, name string, fn func(ctx context.Context) (T, error), opts ...Option) (T, error) {
	var zero T
	o := stepOpts{attempts: 1}
	for _, f := range opts {
		f(&o)
	}
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

	var v T
	for attempt := 1; ; attempt++ {
		v, err = fn(ctx)
		if err == nil {
			break
		}
		if attempt >= o.attempts || ctx.Err() != nil {
			return zero, err
		}
		select {
		case <-time.After(o.backoff << (attempt - 1)):
		case <-ctx.Done():
			return zero, err
		}
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
func Do(ctx context.Context, name string, fn func(ctx context.Context) error, opts ...Option) error {
	_, err := Step(ctx, name, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, opts...)
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
