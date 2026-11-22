package effectus

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Option configures a Step.
type Option func(*stepOpts)

type stepOpts struct {
	attempts int
	backoff  time.Duration
}

const maxBackoff = time.Hour

// Retry makes a step run up to attempts times in total, sleeping backoff,
// 2*backoff, 4*backoff... (capped at one hour) between attempts. Retries
// happen in-process; only the final success is recorded.
func Retry(attempts int, backoff time.Duration) Option {
	return func(o *stepOpts) { o.attempts, o.backoff = attempts, backoff }
}

func (o stepOpts) delay(attempt int) time.Duration {
	d := o.backoff
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	return min(d, maxBackoff)
}

// Step runs fn once, durably. On replay it returns the recorded value
// without calling fn. The name is the step's identity: unique within the
// workflow, stable across deploys. T must be JSON-serializable.
func Step[T any](ctx context.Context, name string, fn func(ctx context.Context) (T, error), opts ...Option) (T, error) {
	var zero T
	r, _ := ctx.Value(ctxKey{}).(*run)
	if r == nil {
		return zero, errors.New("effectus: Step called outside of a workflow")
	}
	o := stepOpts{attempts: 1}
	for _, f := range opts {
		f(&o)
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
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		v, err = fn(ctx)
		if err == nil {
			break
		}
		if attempt >= o.attempts || ctx.Err() != nil {
			return zero, err
		}
		select {
		case <-time.After(o.delay(attempt)):
		case <-ctx.Done():
			return zero, err
		}
	}

	raw, err = json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("effectus: step %q: encode result: %w", name, err)
	}
	// Fenced write: only the current lease holder may record results.
	res, err := r.e.db.ExecContext(context.WithoutCancel(ctx), `
		INSERT INTO steps (workflow_id, name, result)
		SELECT ?, ?, ? WHERE EXISTS (
			SELECT 1 FROM workflows WHERE id = ? AND owner = ? AND status = 'running')`,
		r.id, name, raw, r.id, r.owner)
	if err != nil {
		return zero, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return zero, ErrLeaseLost
	}
	return v, nil
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
