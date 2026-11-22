package effectus_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/olian04/effectus"
)

func TestResumeAfterFailure(t *testing.T) {
	ctx := context.Background()
	e, err := effectus.Open(filepath.Join(t.TempDir(), "log.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	calls := map[string]int{}
	fail := true
	wf := func(ctx context.Context) error {
		name, err := effectus.Step(ctx, "name", func(context.Context) (string, error) {
			calls["name"]++
			return "World", nil
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
	}

	if err := e.Run(ctx, "w1", wf); err == nil {
		t.Fatal("expected failure")
	}
	fail = false
	if err := e.Run(ctx, "w1", wf); err != nil {
		t.Fatal(err)
	}
	if err := e.Run(ctx, "w1", wf); err != nil { // completed: no-op
		t.Fatal(err)
	}
	if calls["name"] != 1 || calls["flaky"] != 2 || calls["greet"] != 1 {
		t.Fatalf("calls = %v", calls)
	}
}

func TestDuplicateNameAndSleep(t *testing.T) {
	ctx := context.Background()
	e, _ := effectus.Open(filepath.Join(t.TempDir(), "log.db"))
	defer e.Close()
	err := e.Run(ctx, "w", func(ctx context.Context) error {
		if err := effectus.Sleep(ctx, "nap", 10*time.Millisecond); err != nil {
			return err
		}
		effectus.Do(ctx, "a", func(context.Context) error { return nil })
		return effectus.Do(ctx, "a", func(context.Context) error { return nil })
	})
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
}
