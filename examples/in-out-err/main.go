// Package main contains the "in-out-err" example program, which reads
// from stdin and writes to both stdout and stderr. It exists to
// demonstrate the use of neilotoole/streamcache.
//
// Usage:
//
//	$ go install github.com/neilotoole/streamcache/examples/in-out-err
//	$ echo "hello world" | in-out-err
//	hello world
//	hello world
//	Read 12 bytes from stdin
//
// Note that the code ignores most errors; don't do that in real life.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/neilotoole/streamcache"
)

// Write stdin to both stdout and stderr.
// Some error handling omitted for brevity.
//
//nolint:errcheck,revive
func main() {
	ctx := context.Background()
	stream := streamcache.New(os.Stdin)

	r1 := stream.NewReader(ctx)
	go func() {
		defer r1.Close()
		io.Copy(os.Stdout, r1)
	}()

	r2 := stream.NewReader(ctx)
	go func() {
		defer r2.Close()
		io.Copy(os.Stderr, r2)
	}()

	stream.Seal()   // Indicate that there'll be no more readers...
	<-stream.Done() // Receives when all readers are closed.

	if err := stream.Err(); err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Read %d bytes from stdin\n", stream.Size())
}
