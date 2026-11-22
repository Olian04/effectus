package effectus_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olian04/effectus"
)

var bg = context.Background()

func open(t *testing.T, path string, opts ...effectus.EngineOption) *effectus.Engine {
	t.Helper()
	e, err := effectus.Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func tmpDB(t *testing.T) string { return filepath.Join(t.TempDir(), "log.db") }

func noop(context.Context, struct{}) error { return nil }

func TestRetryAfterFailureReplaysCompletedSteps(t *testing.T) {
	e := open(t, tmpDB(t))
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

	if err := wf.Run(bg, "w1", "World"); err == nil {
		t.Fatal("expected failure")
	}
	if i, _ := e.Status(bg, "w1"); i.Status != effectus.StatusFailed || i.Error != "boom" || i.Steps != 1 {
		t.Fatalf("status after failure: %+v", i)
	}
	fail = false
	if err := wf.Run(bg, "w1", "World"); err != nil {
		t.Fatal(err)
	}
	if err := wf.Run(bg, "w1", "World"); err != nil { // completed: no-op
		t.Fatal(err)
	}
	if calls["name"] != 1 || calls["flaky"] != 2 || calls["greet"] != 1 {
		t.Fatalf("calls = %v", calls)
	}
	if i, _ := e.Status(bg, "w1"); i.Status != effectus.StatusCompleted || i.Steps != 3 || i.Error != "" {
		t.Fatalf("final status: %+v", i)
	}
}

func TestDuplicateStepName(t *testing.T) {
	e := open(t, tmpDB(t))
	wf := effectus.Register(e, "dup", func(ctx context.Context, _ struct{}) error {
		effectus.Do(ctx, "a", func(context.Context) error { return nil })
		return effectus.Do(ctx, "a", func(context.Context) error { return nil })
	})
	if err := wf.Run(bg, "w", struct{}{}); err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestStepOutsideWorkflow(t *testing.T) {
	_, err := effectus.Step(bg, "x", func(context.Context) (int, error) { return 1, nil })
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRetryOption(t *testing.T) {
	e := open(t, tmpDB(t))
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
	if err := wf.Run(bg, "w", struct{}{}); err != nil || n != 3 {
		t.Fatalf("err=%v n=%d", err, n)
	}

	n = -100
	wf2 := effectus.Register(e, "retry2", func(ctx context.Context, _ struct{}) error {
		return effectus.Do(ctx, "x", func(context.Context) error {
			n++
			return errors.New("always")
		}, effectus.Retry(2, time.Millisecond))
	})
	if err := wf2.Run(bg, "w2", struct{}{}); err == nil || n != -98 {
		t.Fatalf("err=%v n=%d", err, n)
	}
}

func TestLeaseExcludesSecondOwner(t *testing.T) {
	path := tmpDB(t)
	a, b := open(t, path), open(t, path)
	started, release := make(chan struct{}), make(chan struct{})
	wa := effectus.Register(a, "slow", func(ctx context.Context, _ int) error {
		return effectus.Do(ctx, "x", func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	})
	wb := effectus.Register(b, "slow", func(context.Context, int) error { return nil })

	done := make(chan error)
	go func() { done <- wa.Run(bg, "w", 1) }()
	<-started
	if err := wb.Run(bg, "w", 1); !errors.Is(err, effectus.ErrLocked) {
		t.Fatalf("want ErrLocked, got %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInputConflict(t *testing.T) {
	e := open(t, tmpDB(t))
	wf := effectus.Register(e, "k", func(context.Context, string) error { return nil })
	other := effectus.Register(e, "other", func(context.Context, string) error { return nil })
	if err := wf.Run(bg, "w", "a"); err != nil {
		t.Fatal(err)
	}
	if err := wf.Run(bg, "w", "b"); !errors.Is(err, effectus.ErrConflict) {
		t.Fatalf("different input: %v", err)
	}
	if err := other.Run(bg, "w", "a"); !errors.Is(err, effectus.ErrConflict) {
		t.Fatalf("different kind: %v", err)
	}
	if err := wf.Run(bg, "w", "a"); err != nil {
		t.Fatalf("same input again: %v", err)
	}
}

// crash simulates process death mid-workflow: the goroutine exits without
// running the library's outcome bookkeeping.
func crash() { runtime.Goexit() }

func TestResumeAfterCrash(t *testing.T) {
	path := tmpDB(t)
	ttl := effectus.WithLeaseTTL(60 * time.Millisecond)
	var step1, step2 atomic.Int32

	define := func(e *effectus.Engine, doCrash bool) *effectus.Workflow[string] {
		return effectus.Register(e, "order", func(ctx context.Context, item string) error {
			if _, err := effectus.Step(ctx, "reserve", func(context.Context) (string, error) {
				step1.Add(1)
				return item + "-reserved", nil
			}); err != nil {
				return err
			}
			if doCrash {
				crash()
			}
			return effectus.Do(ctx, "ship", func(context.Context) error { step2.Add(1); return nil })
		})
	}

	e1 := open(t, path, ttl)
	w1 := define(e1, true)
	done := make(chan struct{})
	go func() { defer close(done); w1.Run(bg, "o1", "book") }()
	<-done

	e2 := open(t, path, ttl)
	define(e2, false)
	if err := e2.Resume(bg); err != nil {
		t.Fatal(err)
	}
	if step2.Load() != 0 {
		t.Fatal("resumed before lease expiry")
	}
	time.Sleep(100 * time.Millisecond)
	if err := e2.Resume(bg); err != nil {
		t.Fatal(err)
	}
	if step1.Load() != 1 || step2.Load() != 1 {
		t.Fatalf("step1=%d step2=%d", step1.Load(), step2.Load())
	}
	if i, _ := e2.Status(bg, "o1"); i.Status != effectus.StatusCompleted {
		t.Fatalf("%+v", i)
	}
}

func TestHeartbeatKeepsLongStepOwned(t *testing.T) {
	path := tmpDB(t)
	ttl := effectus.WithLeaseTTL(60 * time.Millisecond)
	a, b := open(t, path, ttl), open(t, path, ttl)
	var ran atomic.Int32
	def := func(e *effectus.Engine, d time.Duration) *effectus.Workflow[int] {
		return effectus.Register(e, "long", func(ctx context.Context, _ int) error {
			ran.Add(1)
			return effectus.Do(ctx, "x", func(context.Context) error { time.Sleep(d); return nil })
		})
	}
	wa := def(a, 300*time.Millisecond) // five times the TTL
	wb := def(b, 0)
	done := make(chan error)
	go func() { done <- wa.Run(bg, "w", 1) }()
	time.Sleep(200 * time.Millisecond)
	if err := wb.Run(bg, "w", 1); !errors.Is(err, effectus.ErrLocked) {
		t.Fatalf("want ErrLocked, got %v", err)
	}
	if err := b.Resume(bg); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 {
		t.Fatalf("workflow body ran %d times", ran.Load())
	}
}

func TestShutdownLeavesWorkflowResumable(t *testing.T) {
	e := open(t, tmpDB(t))
	var shipped atomic.Int32
	started := make(chan struct{})
	block := true
	wf := effectus.Register(e, "w", func(ctx context.Context, _ struct{}) error {
		if err := effectus.Do(ctx, "a", func(context.Context) error { return nil }); err != nil {
			return err
		}
		if err := effectus.Do(ctx, "b", func(ctx context.Context) error {
			if block {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}); err != nil {
			return err
		}
		return effectus.Do(ctx, "c", func(context.Context) error { shipped.Add(1); return nil })
	})

	ctx, cancel := context.WithCancel(bg)
	done := make(chan error)
	go func() { done <- wf.Run(ctx, "w", struct{}{}) }()
	<-started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected error from cancelled run")
	}
	if i, _ := e.Status(bg, "w"); i.Status != effectus.StatusRunning {
		t.Fatalf("shutdown must not fail the workflow: %+v", i)
	}
	block = false
	if err := e.Resume(bg); err != nil { // lease was released: immediately resumable
		t.Fatal(err)
	}
	if shipped.Load() != 1 {
		t.Fatal("not resumed")
	}
}

func TestCancel(t *testing.T) {
	e := open(t, tmpDB(t), effectus.WithLeaseTTL(60*time.Millisecond))
	started := make(chan struct{})
	var after atomic.Int32
	wf := effectus.Register(e, "w", func(ctx context.Context, _ struct{}) error {
		if err := effectus.Do(ctx, "a", func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // woken by the heartbeat noticing the cancel
			return nil
		}); err != nil {
			return err
		}
		return effectus.Do(ctx, "b", func(context.Context) error { after.Add(1); return nil })
	})
	done := make(chan error)
	go func() { done <- wf.Run(bg, "w", struct{}{}) }()
	<-started
	if err := e.Cancel(bg, "w"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, effectus.ErrCanceled) {
		t.Fatalf("want ErrCanceled, got %v", err)
	}
	if after.Load() != 0 {
		t.Fatal("step ran after cancel")
	}
	if err := wf.Run(bg, "w", struct{}{}); !errors.Is(err, effectus.ErrCanceled) {
		t.Fatalf("rerun: %v", err)
	}
	if err := e.Cancel(bg, "w"); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := e.Cancel(bg, "nope"); !errors.Is(err, effectus.ErrNotFound) {
		t.Fatalf("%v", err)
	}
}

func TestPanicFailsWorkflow(t *testing.T) {
	e := open(t, tmpDB(t))
	wf := effectus.Register(e, "p", func(ctx context.Context, _ struct{}) error { panic("oops") })
	if err := wf.Run(bg, "w", struct{}{}); err == nil {
		t.Fatal("expected error")
	}
	if i, _ := e.Status(bg, "w"); i.Status != effectus.StatusFailed {
		t.Fatalf("%+v", i)
	}
}

func TestSleepIsDurable(t *testing.T) {
	e := open(t, tmpDB(t))
	slept := 0
	wf := effectus.Register(e, "s", func(ctx context.Context, _ struct{}) error {
		if err := effectus.Sleep(ctx, "nap", 200*time.Millisecond); err != nil {
			return err
		}
		return effectus.Do(ctx, "after", func(context.Context) error {
			slept++
			if slept == 1 {
				return errors.New("fail once")
			}
			return nil
		})
	})
	if err := wf.Run(bg, "w", struct{}{}); err == nil {
		t.Fatal("expected failure")
	}
	start := time.Now()
	if err := wf.Run(bg, "w", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("sleep was not replayed from the recorded deadline")
	}
}

func TestConcurrentSteps(t *testing.T) {
	e := open(t, tmpDB(t))
	wf := effectus.Register(e, "fan", func(ctx context.Context, n int) error {
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = effectus.Do(ctx, fmt.Sprintf("job-%d", i), func(context.Context) error { return nil })
			}()
		}
		wg.Wait()
		return errors.Join(errs...)
	})
	if err := wf.Run(bg, "w", 20); err != nil {
		t.Fatal(err)
	}
	if i, _ := e.Status(bg, "w"); i.Steps != 20 {
		t.Fatalf("%+v", i)
	}
}

func TestStepResultTypesRoundTrip(t *testing.T) {
	type Order struct {
		ID    int
		Items []string
		At    time.Time
	}
	e := open(t, tmpDB(t))
	want := Order{7, []string{"a", "b"}, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	fail := true
	wf := effectus.Register(e, "rt", func(ctx context.Context, _ struct{}) error {
		got, err := effectus.Step(ctx, "o", func(context.Context) (Order, error) { return want, nil })
		if err != nil {
			return err
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("got %v want %v", got, want)
		}
		if fail {
			fail = false
			return errors.New("replay me")
		}
		return nil
	})
	wf.Run(bg, "w", struct{}{})
	if err := wf.Run(bg, "w", struct{}{}); err != nil {
		t.Fatal(err)
	}
}

func TestListAndDelete(t *testing.T) {
	e := open(t, tmpDB(t))
	ok := effectus.Register(e, "ok", func(ctx context.Context, _ struct{}) error {
		return effectus.Do(ctx, "x", func(context.Context) error { return nil })
	})
	bad := effectus.Register(e, "bad", func(context.Context, struct{}) error { return errors.New("no") })
	ok.Run(bg, "a", struct{}{})
	bad.Run(bg, "b", struct{}{})

	all, _ := e.List(bg)
	if len(all) != 2 || all[0].ID != "a" || all[1].ID != "b" {
		t.Fatalf("%+v", all)
	}
	failed, _ := e.List(bg, effectus.StatusFailed)
	if len(failed) != 1 || failed[0].ID != "b" {
		t.Fatalf("%+v", failed)
	}
	if err := e.Delete(bg, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Status(bg, "a"); !errors.Is(err, effectus.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	if err := e.Delete(bg, "a"); !errors.Is(err, effectus.ErrNotFound) {
		t.Fatalf("%v", err)
	}
	// Deleted id can be reused from scratch.
	if err := ok.Run(bg, "a", struct{}{}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRefusesRunning(t *testing.T) {
	e := open(t, tmpDB(t))
	started, release := make(chan struct{}), make(chan struct{})
	wf := effectus.Register(e, "w", func(ctx context.Context, _ struct{}) error {
		return effectus.Do(ctx, "x", func(context.Context) error { close(started); <-release; return nil })
	})
	done := make(chan error)
	go func() { done <- wf.Run(bg, "w", struct{}{}) }()
	<-started
	if err := e.Delete(bg, "w"); err == nil {
		t.Fatal("deleted a running workflow")
	}
	close(release)
	<-done
}

func TestSchemaVersionGuard(t *testing.T) {
	path := tmpDB(t)
	e := open(t, path)
	e.Close()
	// A log written by a newer library must be refused, not corrupted.
	raw, err := openRaw(path)
	if err != nil {
		t.Fatal(err)
	}
	raw.Exec(`PRAGMA user_version = 99`)
	raw.Close()
	if _, err := effectus.Open(path); err == nil {
		t.Fatal("expected schema version error")
	}
}
