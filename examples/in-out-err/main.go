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
	"sync"

	"github.com/neilotoole/streamcache"
)

// Write stdin to both stdout and stderr.
func main() {
	ctx := context.Background()
	stream := streamcache.New(os.Stdin)

	r1 := stream.NewReader(ctx)
	r2 := stream.NewReader(ctx)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		io.Copy(os.Stdout, r1)
		r1.Close()
	}()
	go func() {
		defer wg.Done()
		io.Copy(os.Stderr, r2)
		r2.Close()
	}()
	wg.Wait()

	if stream.Err() != nil && !errors.Is(stream.Err(), io.EOF) {
		fmt.Fprintln(os.Stderr, "error:", stream.Err())
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "Read %d bytes from stdin\n", stream.Size())
}
