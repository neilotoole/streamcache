// Package main provides the "multicase" example CLI that reads from
// stdin and outputs each line in lower, upper, and title case. Usage:
//
//	$ cat FILE | multicase
//	# Or interactive mode (user enters input)
//	$ multicase
//
// "multicase" exists to demonstrate use of neilotoole/streamcache.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/neilotoole/streamcache"
)

// main sets up the CLI, and invokes exec to do the actual work.
func main() {
	ctx, cancelFn := context.WithCancel(context.Background())
	var err error
	defer func() {
		cancelFn()
		if err != nil {
			os.Exit(1)
		}
	}()

	go func() {
		stopCh := make(chan os.Signal, 1)
		signal.Notify(stopCh, os.Interrupt)

		<-stopCh
		cancelFn()
	}()

	var fi os.FileInfo
	if fi, err = os.Stdin.Stat(); err != nil {
		printErr(err)
		return
	}

	if os.ModeNamedPipe&fi.Mode() == 0 && fi.Size() == 0 {
		// No data on stdin, thus interactive mode: print a prompt.
		fmt.Fprintln(os.Stdout, colorize(ansiFaint, "multicase: enter text and press [ENTER]"))
	}

	if err = exec(ctx, os.Stdin, os.Stdout); err != nil {
		printErr(err)
	}
}

func exec(ctx context.Context, in io.Reader, out io.Writer) error {
	toUpper := func(s string) string {
		return colorize(ansiRed, strings.ToUpper(s))
	}
	toLower := func(s string) string {
		return colorize(ansiGreen, strings.ToLower(s))
	}
	toTitle := func(s string) string {
		return colorize(ansiBlue, strings.Title(s)) //nolint:staticcheck
	}
	transforms := []func(string) string{toUpper, toLower, toTitle}

	stream := streamcache.New(in)
	rdrs := make([]*streamcache.Reader, len(transforms))
	for i := range rdrs {
		rdrs[i] = stream.NewReader(ctx)
	}

	// Seal the stream to indicate no more readers.
	stream.Seal()

	errCh := make(chan error, 1)
	for i := range transforms {
		go func(i int) {
			r, t := rdrs[i], transforms[i]
			defer func() {
				if closeErr := r.Close(); closeErr != nil {
					errCh <- closeErr
				}
			}()

			sc := bufio.NewScanner(r)
			for sc.Scan() {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
				}

				text := sc.Text()
				fmt.Fprintln(out, t(text))
			}

			if err := sc.Err(); err != nil {
				errCh <- err
			}
		}(i)
	}

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-errCh:
	case <-stream.ReadersDone():
		err = stream.Err()
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}
	return err
}

func colorize(ansi, s string) string {
	return ansi + s + ansiReset
}

const (
	ansiFaint = "\033[2m"
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
)

func printErr(err error) {
	fmt.Fprintln(os.Stderr, colorize(ansiRed, "multicase: error: "+err.Error()))
}
