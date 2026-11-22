package effectus_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/olian04/effectus"
)

// TestKill9 kills a real child process mid-workflow, then resumes in this one.
func TestKill9(t *testing.T) {
	if path := os.Getenv("EFFECTUS_CHILD_DB"); path != "" {
		e, err := effectus.Open(path, effectus.WithLeaseTTL(100*time.Millisecond))
		if err != nil {
			os.Exit(2)
		}
		wf := effectus.Register(e, "k", func(ctx context.Context, _ struct{}) error {
			if err := effectus.Do(ctx, "one", func(context.Context) error {
				return os.WriteFile(path+".one", []byte("x"), 0o644)
			}); err != nil {
				return err
			}
			return effectus.Do(ctx, "two", func(context.Context) error {
				os.WriteFile(path+".ready", nil, 0o644)
				time.Sleep(time.Hour) // killed here
				return nil
			})
		})
		wf.Run(context.Background(), "w", struct{}{})
		return
	}

	path := tmpDB(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestKill9$")
	cmd.Env = append(os.Environ(), "EFFECTUS_CHILD_DB="+path)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		if _, err := os.Stat(path + ".ready"); err == nil {
			break
		}
		if i > 200 {
			cmd.Process.Kill()
			t.Fatal("child never reached step two")
		}
		time.Sleep(25 * time.Millisecond)
	}
	cmd.Process.Kill() // SIGKILL: no deferred cleanup runs
	cmd.Wait()
	os.Remove(path + ".one")

	e := open(t, path, effectus.WithLeaseTTL(100*time.Millisecond))
	var twoRan bool
	effectus.Register(e, "k", func(ctx context.Context, _ struct{}) error {
		if err := effectus.Do(ctx, "one", func(context.Context) error {
			t.Error("step one must be replayed, not re-run")
			return nil
		}); err != nil {
			return err
		}
		return effectus.Do(ctx, "two", func(context.Context) error { twoRan = true; return nil })
	})
	time.Sleep(150 * time.Millisecond) // let the dead child's lease expire
	if err := e.Resume(bg); err != nil {
		t.Fatal(err)
	}
	if !twoRan {
		t.Fatal("step two did not run after resume")
	}
	if _, err := os.Stat(path + ".one"); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Fatal("step one effect repeated")
	}
}
