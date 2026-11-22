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
	"sync/atomic"
	"time"
)

// Workflow is a registered, typed workflow definition.
type Workflow[In any] struct {
	e    *Engine
	kind string
	fn   func(ctx context.Context, in In) error
}

// Register defines a workflow kind. The input is persisted with the
// workflow so that Resume can restart it after a crash; it must be
// JSON-serializable. Register every kind before calling Resume, and keep
// kind names stable across deploys.
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

// Run executes the workflow identified by id and blocks until it finishes.
//
// Run is idempotent per id: if the workflow already completed it returns nil
// without calling the function; otherwise the function runs from the top,
// replaying recorded steps. Reusing an id with a different input returns
// ErrConflict; a canceled workflow returns ErrCanceled; one owned by a live
// process returns ErrLocked.
//
// If ctx is cancelled, Run returns and the workflow stays resumable.
func (w *Workflow[In]) Run(ctx context.Context, id string, in In) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("effectus: encode input: %w", err)
	}
	return w.e.exec(ctx, w.kind, id, raw, func(ctx context.Context) error { return w.fn(ctx, in) })
}

// Resume re-runs every workflow that a crashed (or shut down) owner left
// running and whose kind is registered, and blocks until they finish.
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
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
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
			err := start(ctx, j.id, j.input)
			if err != nil && !errors.Is(err, ErrLocked) && !errors.Is(err, ErrCanceled) {
				emu.Lock()
				errs = append(errs, fmt.Errorf("workflow %q: %w", j.id, err))
				emu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func nowMs() int64 { return time.Now().UnixMilli() }

func randID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// acquire takes the lease, or explains why it could not.
// It returns acquired=false, nil when the workflow already completed.
func (e *Engine) acquire(ctx context.Context, owner, kind, id string, input []byte) (acquired bool, err error) {
	res, err := e.db.ExecContext(ctx, `
		INSERT INTO workflows (id, kind, input, status, owner, lease_until)
		VALUES (?, ?, ?, 'running', ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = 'running', error = '',
			owner = excluded.owner, lease_until = excluded.lease_until
		WHERE workflows.status IN ('running', 'failed')
		  AND workflows.lease_until < ?
		  AND workflows.kind = excluded.kind
		  AND workflows.input = excluded.input`,
		id, kind, input, owner, nowMs()+e.leaseTTL.Milliseconds(), nowMs())
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, nil
	}
	var (
		status Status
		k      string
		in     []byte
	)
	if err := e.db.QueryRowContext(ctx,
		`SELECT status, kind, input FROM workflows WHERE id = ?`, id).Scan(&status, &k, &in); err != nil {
		return false, err
	}
	switch {
	case k != kind || string(in) != string(input):
		return false, ErrConflict
	case status == StatusCompleted:
		return false, nil
	case status == StatusCanceled:
		return false, ErrCanceled
	}
	return false, ErrLocked
}

func (e *Engine) exec(ctx context.Context, kind, id string, input []byte, fn func(ctx context.Context) error) error {
	owner := randID()
	if acquired, err := e.acquire(ctx, owner, kind, id, input); err != nil || !acquired {
		return err
	}

	// Heartbeat: keep the lease alive; stop the workflow if it is lost.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
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
					`UPDATE workflows SET lease_until = ?
					 WHERE id = ? AND owner = ? AND status = 'running'`,
					nowMs()+e.leaseTTL.Milliseconds(), id, owner)
				if err != nil {
					continue // transient; the lease is still ours until it expires
				}
				if n, _ := r.RowsAffected(); n == 0 {
					lost.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	werr := e.call(wctx, &run{e: e, id: id, owner: owner, seen: map[string]bool{}}, id, fn)
	cancel()
	<-hbDone

	// Record the outcome even though ctx may be cancelled. If the process
	// dies before this point, status stays 'running' with an expiring lease,
	// which Resume picks up.
	bg := context.WithoutCancel(ctx)
	if lost.Load() {
		return e.whyLost(bg, id)
	}
	if werr != nil {
		if ctx.Err() != nil { // shutdown: hand the workflow back, don't fail it
			e.db.ExecContext(bg, `UPDATE workflows SET lease_until = 0
				WHERE id = ? AND owner = ? AND status = 'running'`, id, owner)
			return werr
		}
		res, err := e.db.ExecContext(bg, `UPDATE workflows SET status = 'failed', error = ?, lease_until = 0
			WHERE id = ? AND owner = ? AND status = 'running'`, werr.Error(), id, owner)
		if err == nil {
			if n, _ := res.RowsAffected(); n == 0 {
				return e.whyLost(bg, id)
			}
		}
		return werr
	}
	res, err := e.db.ExecContext(bg, `UPDATE workflows SET status = 'completed', lease_until = 0
		WHERE id = ? AND owner = ? AND status = 'running'`, id, owner)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return e.whyLost(bg, id)
	}
	return nil
}

// call runs the workflow function, turning a panic into a workflow failure.
// (runtime.Goexit is deliberately not intercepted.)
func (e *Engine) call(ctx context.Context, r *run, id string, fn func(context.Context) error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("effectus: workflow %q panicked: %v", id, p)
		}
	}()
	return fn(context.WithValue(ctx, ctxKey{}, r))
}

func (e *Engine) whyLost(ctx context.Context, id string) error {
	var s Status
	err := e.db.QueryRowContext(ctx, `SELECT status FROM workflows WHERE id = ?`, id).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && s != StatusCanceled) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	return ErrCanceled
}

type run struct {
	e     *Engine
	id    string
	owner string
	mu    sync.Mutex
	seen  map[string]bool
}

type ctxKey struct{}
