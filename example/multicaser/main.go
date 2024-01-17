package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/neilotoole/streamcache"
	"golang.org/x/sync/errgroup"
	"io"
	"os"
	"os/signal"
	"strings"
)

const (
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
)

func main() {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	go func() {
		stopCh := make(chan os.Signal, 1)
		signal.Notify(stopCh, os.Interrupt)

		<-stopCh
		cancelFn()
	}()

	fmt.Fprintf(os.Stdin, "multicaser: reading from stdin, writing to stdout\n\n")
	if err := exec2(ctx, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, ansiRed+"error: "+err.Error()+ansiReset)
		os.Exit(1)
	}
}

func exec2(ctx context.Context, in io.Reader, out io.Writer) error {
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
	cache := streamcache.New(in)
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
				if err := r.Close(); err != nil {
					panic(err)
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errCh:
		return err
	case <-cache.Done():
		return cache.Err()
	}

}

func exec(ctx context.Context, in io.Reader, out io.Writer) error {
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
	cache := streamcache.New(in)
	rdrs := make([]*streamcache.Reader, len(transforms))
	var err error
	for i := range rdrs {
		rdrs[i], err = cache.NewReader(ctx)
		if err != nil {
			return err
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	for i, t := range transforms {
		i := i
		t := t
		//r, err := cache.NewReader(ctx)
		//if err != nil {
		//	return err
		//}
		g.Go(func() error {
			r := rdrs[i]
			defer func() {
				closeErr := r.Close()
				if closeErr != nil {
					panic(closeErr)
				}
			}()

			sc := bufio.NewScanner(r)
			for sc.Scan() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				fmt.Fprintln(out, t(sc.Text()))
			}

			return sc.Err()
		})
	}

	if err := cache.Seal(); err != nil {
		return err
	}

	return g.Wait()
}
