# streamcache: in-memory caching stream reader

[![Go Reference](https://pkg.go.dev/badge/github.com/neilotoole/streamcache.svg)](https://pkg.go.dev/github.com/neilotoole/streamcache)
[![Go Report Card](https://goreportcard.com/badge/neilotoole/streamcache)](https://goreportcard.com/report/neilotoole/streamcache)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/neilotoole/streamcache/blob/master/LICENSE)
![Workflow](https://github.com/neilotoole/streamcache/actions/workflows/go.yml/badge.svg)
[![codecov](https://codecov.io/gh/neilotoole/streamcache/graph/badge.svg)](https://codecov.io/gh/neilotoole/streamcache)


Package [`streamcache`](https://pkg.go.dev/github.com/neilotoole/streamcache)
implements a Go in-memory byte cache mechanism that allows multiple callers to
read some or all of the contents of a source `io.Reader`, while only reading
from the source reader once. When only the final reader remains, the cache is
discarded and the final reader reads directly from the source. This is particularly
useful for scenarios where multiple readers may wish to sample the start of a
stream, but only one reader will read the entire stream.

Let's say we have a program [`typedetect`](./examples/typedetect),
and we're reading from `stdin`. For example:

```shell
$ cat myfile.ext | typedetect  
```

In this scenario, `typedetect` wants to detect
and print the type of data in the file/pipe, and then print the contents.
That detection sampling could be done in a separate goroutine per sampler type.
The input file could be, let's say, a JSON file, or an XML file.

The obvious approach is to inspect the first few tokens of the
input, and check if the tokens are either valid JSON or valid XML.
After that process, let's say we want to dump out a preview of the file contents.

Package `streamcache` provides a facility to create a `Stream` from an
underlying `io.Reader` (`os.Stdin` in this scenario), and spawn multiple
readers, each of which can operate independently, in their own
goroutines if desired. The underlying source (again, `os.Stdin` in this
scenario) will only once be read from, but its data is available to
multiple readers, because that data is cached in memory.

That is, until there's only one final reader left, (after invoking
`Stream.Seal`), at which point the cache is discarded, and
the final `Reader` reads directly from the underlying source.

## Usage

> [!NOTE]
> Requires Go 1.26+.

Add to your `go.mod` via `go get`:

```shell
go get github.com/neilotoole/streamcache
```

Here's a simple [example](./examples/in-out-err) that copies the contents
of `stdin` to `stdout` and `stderr`, and prints the number of bytes read.

```go
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
```

Executing the above program:

```shell
$ go install github.com/neilotoole/streamcache/examples/in-out-err
$ echo "hello world" | in-out-err
hello world
hello world
Read 12 bytes from stdin
```

## Limiting cache size

By default, a `Stream` caches in memory every byte read from the source until
the stream is sealed down to its final reader. For an unbounded source, use the
`MaxCacheSize` option to cap how much the stream will buffer:

```go
ctx := context.Background()
stream := streamcache.New(os.Stdin, streamcache.MaxCacheSize(1_000_000))

r := stream.NewReader(ctx)
defer r.Close()
if _, err := io.Copy(os.Stdout, r); err != nil {
    if errors.Is(err, streamcache.ErrCacheLimit) {
        // The source exceeded the 1 MB cache limit.
    }
}
```

If the source yields more than the configured limit, `Reader.Read` returns
`streamcache.ErrCacheLimit` and the stream enters a terminal error state
(`Stream.Err` returns `streamcache.ErrCacheLimit`). If the read that crosses the
limit also returns a genuine (non-`io.EOF`) error from the source, that source
error is reported instead. The limit does not apply to the final reader once the
stream is sealed, since that reader reads directly from the source and does not
use the cache.

## Examples

- [`in-out-err`](./examples/in-out-err): copy `stdin` to both `stdout` and `stderr`.
- [`typedetect`](./examples/typedetect): detect the type of input data, and print the head and tail
  of the contents.
  ![streamcache_typedetect.png](examples/typedetect/streamcache_typedetect.png)
- [`multicase`](./examples/multicase): transform each line of input to upper, lower, and title case.
  ![streamcache_multicase.png](examples/multicase/streamcache_multicase.png)

## Related work

- [`djherbis/fscache`](https://github.com/djherbis/fscache)
- [`sq`](https://github.com/neilotoole/sq) uses `streamcache` to stream stdin / HTTP response bodies,
  allowing `sq` to being processing data on the fly.
  ![sq streamcache](https://github.com/neilotoole/sq/blob/master/.images/sq_inspect_remote_s3.png)
- [`fifomu`](https://github.com/neilotoole/fifomu) is a FIFO mutex, used by `streamcache`, which in turn is used by [`sq`](https://github.com/neilotoole/sq).

## Changelog

### Unreleased

#### Added

- `MaxCacheSize` functional option for `New`, capping the number of bytes
  buffered in the in-memory cache, plus the `ErrCacheLimit` sentinel returned
  (and a terminal stream state) when the source exceeds the limit. See
  [Limiting cache size](#limiting-cache-size).
  ([#8](https://github.com/neilotoole/streamcache/pull/8), closes
  [#5](https://github.com/neilotoole/streamcache/issues/5))
