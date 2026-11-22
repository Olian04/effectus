# effectus

Durable execution made simple. A workflow is a plain Go function; every side
effect inside it goes through `Step`. Results are logged in SQLite, so a
crashed workflow resumes where it stopped.

```go
e, _ := effectus.Open("app.db")

order := effectus.Register(e, "order", func(ctx context.Context, item string) error {
    id, err := effectus.Step(ctx, "reserve", func(ctx context.Context) (string, error) {
        return reserve(item)
    }, effectus.Retry(3, time.Second))
    if err != nil { return err }

    if err := effectus.Sleep(ctx, "cooldown", 24*time.Hour); err != nil { return err }

    return effectus.Do(ctx, "ship", func(ctx context.Context) error { return ship(id) })
})

e.Resume(ctx)                        // on startup: finish workflows a crashed process left behind
err := order.Run(ctx, "order-42", "book") // idempotent per id
```

## Semantics

- **Steps are keyed by name**, unique per workflow and stable across deploys. In loops use `fmt.Sprintf("send-%d", i)`.
- **A successful step never runs again.** A step that crashed mid-flight runs again: delivery is *at-least-once*, so make external effects idempotent.
- **Only successes are recorded.** A failing step (after `Retry`) fails the workflow; `Run` again retries from that step. `Resume` only picks up crashed workflows, not failed ones.
- **One owner per workflow id**, via a lease with heartbeat. A second `Run` gets `ErrLocked`; a crashed owner's lease expires (`WithLeaseTTL`, default 10s).
- **Code between steps must be deterministic.** Put `time.Now`, randomness and I/O inside steps.
- Inputs and step results are stored as JSON.
