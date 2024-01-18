package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/neilotoole/sq/libsq/core/lg/devlog"

	"github.com/neilotoole/streamcache"
)

func getLogFile() (*os.File, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	name := filepath.Join(home, "multicase.log")
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)
}

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

	f, err := getLogFile()
	if err != nil {
		printErr(err)
		return
	}
	defer f.Close()
	log := slog.New(devlog.NewHandler(f, slog.LevelDebug))

	fmt.Fprintln(os.Stdin, colorize(ansiFaint, "multicase: enter text and press [RETURN]"))

	if err = exec(ctx, log, os.Stdin, os.Stdout); err != nil {
		printErr(err)
	}
}

func exec(ctx context.Context, log *slog.Logger, in io.Reader, out io.Writer) error {
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

	// cache := streamcache.New(log, &prompter{in: in, out: out})
	cache := streamcache.New(log, in)
	rdrs := make([]*streamcache.Reader, len(transforms))
	var err error
	for i := range rdrs {
		if rdrs[i], err = cache.NewReader(ctx); err != nil {
			return err
		}
	}
	rdrs[0].Name = "red-upper"
	rdrs[1].Name = "blue-lower"
	rdrs[2].Name = "green-title"

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
		err = ctx.Err()
	case err = <-errCh:
	case <-cache.Done():
		err = cache.Err()
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}
	return err
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
	fmt.Fprintln(pr.out, ansiFaint+"Enter text and press [RETURN]"+ansiReset)
	return pr.in.Read(p)
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
	fmt.Fprintln(os.Stderr, colorize(ansiRed, "error: "+err.Error()))
}
