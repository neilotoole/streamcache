package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/neilotoole/sq/libsq/core/lg/devlog"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/neilotoole/streamcache"
)

const (
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
)

func printErr(err error) {
	fmt.Fprintf(os.Stderr, ansiRed+"error: "+err.Error()+ansiReset+"\n")
}

func getLogFile() (*os.File, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	name := filepath.Join(home, "multicaser.log")
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
}

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())

	go func() {
		stopCh := make(chan os.Signal, 1)
		signal.Notify(stopCh, os.Interrupt)

		<-stopCh
		cancelFn()
	}()

	f, err := getLogFile()
	if err != nil {
		printErr(err)
		os.Exit(1)
	}
	defer f.Close()
	log := slog.New(devlog.NewHandler(f, slog.LevelDebug))

	fmt.Fprintf(os.Stdin, "multicaser: enter text and press [RETURN]\n")
	if err := exec(ctx, log, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, ansiRed+"error: "+err.Error()+ansiReset)
		cancelFn()
		os.Exit(1)
	}
	cancelFn()
}

func exec(ctx context.Context, log *slog.Logger, in io.Reader, out io.Writer) error {
	upper := func(s string) string {
		return ansiRed + strings.ToUpper(s) + ansiReset
	}

	lower := func(s string) string {
		return ansiGreen + strings.ToLower(s) + ansiReset
	}

	regular := func(s string) string {
		return ansiBlue + s + ansiReset
	}

	transforms := []func(string) string{upper, lower, regular}

	cache := streamcache.New(log, &prompter{in: in, out: out})
	//cache := streamcache.New(log, in)
	rdrs := make([]*streamcache.Reader, len(transforms))
	var err error
	for i := range rdrs {
		if rdrs[i], err = cache.NewReader(ctx); err != nil {
			return err
		}
	}
	rdrs[0].Name = "red-upper"
	rdrs[1].Name = "blue-lower"
	rdrs[2].Name = "green-regular"

	if err = cache.Seal(); err != nil {
		return err
	}

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

			if err = sc.Err(); err != nil {
				errCh <- err
			}
		}(i)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		return err
	case <-cache.Done():
		return cache.Err()
	}
}

var _ io.Reader = (*prompter)(nil)

// prompter is an io.Reader that writes a prompt to out before
// reading from in.
type prompter struct {
	in  io.Reader
	out io.Writer
}

// Read implements io.Reader. It renders a prompt to out before
// reading from in.
func (pr *prompter) Read(p []byte) (n int, err error) {
	fmt.Fprint(pr.out, "> ")
	return pr.in.Read(p)
}
