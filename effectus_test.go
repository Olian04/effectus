package effectus_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olian04/effectus"
)

func open(t *testing.T, path string, opts ...effectus.EngineOption) *effectus.Engine {
	t.Helper()
	e, err := effectus.Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestRetryAfterFailureReplaysCompletedSteps(t *testing.T) {
	ctx := context.Background()
	e := open(t, filepath.Join(t.TempDir(), "log.db"))

	calls := map[string]int{}
	fail := true
	wf := effectus.Register(e, "greet", func(ctx context.Context, who string) error {
		name, err := effectus.Step(ctx, "name", func(context.Context) (string, error) {
			calls["name"]++
			return who, nil
		})
		if err != nil {
			return err
		}
		if err := effectus.Do(ctx, "flaky", func(context.Context) error {
			calls["flaky"]++
			if fail {
				return errors.New("boom")
			}
			return nil
		}); err != nil {
			return err
		}
		return effectus.Do(ctx, "greet", func(context.Context) error {
			calls["greet"]++
			if name != "World" {
				t.Errorf("name = %q", name)
			}
			return nil
		})
	})

	if err := wf.Run(ctx, "w1", "World"); err == nil {
		t.Fatal("expected failure")
	}
	fail = false
	if err := wf.Run(ctx, "w1", "World"); err != nil {
		t.Fatal(err)
	}
	if err := wf.Run(ctx, "w1", "World"); err != nil { // completed: no-op
		t.Fatal(err)
	}
	if calls["name"] != 1 || calls["flaky"] != 2 || calls["greet"] != 1 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestDuplicateNameAndSleep(t *testing.T) {
	e := open(t, filepath.Join(t.TempDir(), "log.db"))
	wf := effectus.Register(e, "dup", func(ctx context.Context, _ struct{}) error {
		if err := effectus.Sleep(ctx, "nap", 10*time.Millisecond); err != nil {
			return err
		}
		effectus.Do(ctx, "a", func(context.Context) error { return nil })
		return effectus.Do(ctx, "a", func(context.Context) error { return nil })
	})
	if err := wf.Run(context.Background(), "w", struct{}{}); err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestRetryOption(t *testing.T) {
	e := open(t, filepath.Join(t.TempDir(), "log.db"))
	var n int
	wf := effectus.Register(e, "retry", func(ctx context.Context, _ struct{}) error {
		return effectus.Do(ctx, "x", func(context.Context) error {
			n++
			if n < 3 {
				return errors.New("transient")
			}
			return nil
		}, effectus.Retry(3, time.Millisecond))
	})
	if err := wf.Run(context.Background(), "w", struct{}{}); err != nil || n != 3 {
		t.Fatalf("err=%v n=%d", err, n)
	}
}

func TestLeaseExcludesSecondOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.db")
	a, b := open(t, path), open(t, path)
	started, release := make(chan struct{}), make(chan struct{})
	def := func(e *effectus.Engine) *effectus.Workflow[int] {
		return effectus.Register(e, "slow", func(ctx context.Context, _ int) error {
			return effectus.Do(ctx, "x", func(context.Context) error {
				close(started)
				<-release
				return nil
			})
		})
	}
	wa := def(a)
	wb := effectus.Register(b, "slow", func(context.Context, int) error { return nil })

	done := make(chan error)
	go func() { done <- wa.Run(context.Background(), "w", 1) }()
	<-started
	if err := wb.Run(context.Background(), "w", 1); !errors.Is(err, effectus.ErrLocked) {
		t.Fatalf("want ErrLocked, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestResumeAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.db")
	ttl := effectus.WithLeaseTTL(60 * time.Millisecond)
	var step1, step2 atomic.Int32

	define := func(e *effectus.Engine, crash bool) *effectus.Workflow[string] {
		return effectus.Register(e, "order", func(ctx context.Context, item string) error {
			if _, err := effectus.Step(ctx, "reserve", func(context.Context) (string, error) {
				step1.Add(1)
				return item + "-reserved", nil
			}); err != nil {
				return err
			}
			if crash {
				panic("simulated process death")
			}
			return effectus.Do(ctx, "ship", func(context.Context) error { step2.Add(1); return nil })
		})
	}

	// Process 1 dies between steps.
	e1 := open(t, path, ttl)
	w1 := define(e1, true)
	func() {
		defer func() { recover() }()
		w1.Run(context.Background(), "o1", "book")
	}()

	// Process 2 starts up, registers, resumes. Lease must expire first.
	e2 := open(t, path, ttl)
	define(e2, false)
	if err := e2.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if step2.Load() != 0 { // lease not yet expired: nothing resumed
		t.Fatal("resumed before lease expiry")
	}
	time.Sleep(100 * time.Millisecond)
	if err := e2.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if step1.Load() != 1 || step2.Load() != 1 {
		t.Fatalf("step1=%d step2=%d", step1.Load(), step2.Load())
	}
}
