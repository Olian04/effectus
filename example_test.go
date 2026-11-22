package effectus_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/olian04/effectus"
)

func Example() {
	dir, _ := os.MkdirTemp("", "effectus")
	defer os.RemoveAll(dir)
	e, _ := effectus.Open(filepath.Join(dir, "app.db"))
	defer e.Close()

	hello := effectus.Register(e, "hello", func(ctx context.Context, greeting string) error {
		name, err := effectus.Step(ctx, "name", func(context.Context) (string, error) {
			return "World", nil
		})
		if err != nil {
			return err
		}
		return effectus.Do(ctx, "print", func(context.Context) error {
			fmt.Printf("%s, %s!\n", greeting, name)
			return nil
		})
	})

	hello.Run(context.Background(), "hello-1", "Hello")
	hello.Run(context.Background(), "hello-1", "Hello") // completed: does nothing
	// Output: Hello, World!
}
