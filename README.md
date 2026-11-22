# effectus

Durable execution for Go, made simple. A workflow is a plain Go function; every
side effect inside it goes through `Step`. Results are logged in SQLite, so a
crashed workflow resumes where it stopped. No server, no DSL, no code
generation: one library, one database file.

```go
e, _ := effectus.Open("app.db")
defer e.Close()

order := effectus.Register(e, "order", func(ctx context.Context, item string) error {
    id, err := effectus.Step(ctx, "reserve", func(ctx context.Context) (string, error) {
        return reserve(item)
    }, effectus.Retry(3, time.Second))
    if err != nil { return err }

    if err := effectus.Sleep(ctx, "cooldown", 24*time.Hour); err != nil { return err }

    return effectus.Do(ctx, "ship", func(ctx context.Context) error { return ship(id) })
})

e.Resume(ctx)                              // on startup: finish what a crashed process left behind
err := order.Run(ctx, "order-42", "book")  // idempotent per id
```

## The model

There is one idea: **a step's result is recorded under `(workflow id, step name)`.**
Everything else follows from it.

| You want | You write |
| --- | --- |
| a durable side effect with a result | `Step(ctx, name, fn)` |
| a durable side effect, no result | `Do(ctx, name, fn)` |
| a durable timer | `Sleep(ctx, name, d)` (the deadline is a recorded step) |
| retries | `Step(..., Retry(n, backoff))` |
| fan-out | call `Step` from goroutines; names make it safe |
| recover after a crash | `Engine.Resume(ctx)` |
| inspect / stop / clean up | `Status`, `List`, `Cancel`, `Delete` |

## Semantics

- **Steps are keyed by name**: unique per workflow, stable across deploys. In loops use `fmt.Sprintf("send-%d", i)`. A duplicate name is an error.
- **A successful step never runs again.** A step that crashed mid-flight runs again, so delivery is *at-least-once*: make external effects idempotent (use the workflow id and step name as an idempotency key).
- **Only successes are recorded.** A step that fails (after `Retry`) fails the workflow; `Run` again retries from that step. `Resume` only picks up crashed workflows, never failed ones.
- **One owner per workflow id**, via a lease with heartbeat. A second `Run` gets `ErrLocked`. A crashed owner's lease expires (`WithLeaseTTL`, default 10s) and `Resume` takes over. Step results are written fenced by the lease, so an owner that lost it cannot corrupt the log.
- **Shutdown is not failure.** Cancelling the context passed to `Run` leaves the workflow `running` and releases its lease, so `Resume` can continue it immediately.
- **`Run` is idempotent per id.** Completed: returns nil. Same id with a different kind or input: `ErrConflict`. Canceled: `ErrCanceled`.
- **A panic in workflow code fails the workflow** (it does not crash the process).
- **Code between steps must be deterministic.** On replay it must take the same path. Put `time.Now`, randomness and I/O inside steps. Don't rename or reorder steps for workflows that are in flight.
- Inputs and step results are stored as JSON, so you can read the log with the `sqlite3` CLI.

## Operations

- Any number of processes may share one database file (WAL mode). Each runs `Register` + `Resume` on startup.
- `Cancel` stops a workflow for good; a live owner notices within a heartbeat (TTL/3).
- `Delete` removes a finished workflow and its steps. There is no automatic retention; delete (or `List` + `Delete`) on your own schedule.
- The log carries a schema version (`PRAGMA user_version`); a file written by a newer library is refused.

## Non-goals

No distributed queue, no cron scheduler, no workflow outputs (return data through
steps or your own storage), no signals/queries. Keeping the model to a single idea
is the point.
