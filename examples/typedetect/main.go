package main

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/net/html"

	"github.com/neilotoole/sq/libsq/core/lg/devlog"

	"github.com/neilotoole/streamcache"
)

func getLogFile() (*os.File, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	name := filepath.Join(home, "typedetect.log")
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

	logFile, err := getLogFile()
	if err != nil {
		printErr(err)
		return
	}
	defer logFile.Close()
	log := slog.New(devlog.NewHandler(logFile, slog.LevelDebug))

	// const inFileName = "/Users/neilotoole/work/sq/streamcache/examples/typedetect/sqio.html"
	// const inFileName = "/Users/neilotoole/work/sq/streamcache/examples/typedetect/actor.json"
	const inFileName = "/Users/neilotoole/work/sq/streamcache/examples/typedetect/actor.jsonl"

	in, err := os.Open(inFileName)
	// in, err := os.Open("/Users/neilotoole/work/sq/streamcache/examples/typedetect/actor.json")
	if err != nil {
		printErr(err)
		return
	}

	// fmt.Fprintln(os.Stdout, colorize(ansiFaint, "multicase: enter text and press [ENTER]"))

	if err = exec(ctx, log, in, os.Stdout); err != nil {
		printErr(err)
	}
}

const (
	tokenThreshold  = 10
	numPreviewLines = 5
	maxLineWidth    = 80
)

// detectFunc is a function that detects the type of data on rc.
// On success, the function returns a non-empty string, e.g. "json"
// or "xml". On failure, the function returns empty string. The
// function must close rc in either case.
type detectFunc func(ctx context.Context, rc io.ReadCloser) (typ string)

func exec(ctx context.Context, log *slog.Logger, in io.Reader, out io.Writer) error {
	detectors := []detectFunc{detectJSON, detectXML, detectHTML}

	cache := streamcache.New(log, in)
	rdrs := make([]*streamcache.Reader, len(detectors))
	var err error
	for i := range detectors {
		if rdrs[i], err = cache.NewReader(ctx); err != nil {
			return err
		}
	}

	previewRdr, err := cache.NewReader(ctx)
	if err != nil {
		return err
	}
	defer previewRdr.Close()

	if err = cache.Seal(); err != nil {
		return err
	}

	detectedCh := make(chan string, len(detectors))
	wg := &sync.WaitGroup{}
	wg.Add(len(detectors))
	for i := range detectors {
		go func(i int) {
			defer wg.Done()
			r, detector := rdrs[i], detectors[i]
			defer r.Close()

			typ := detector(ctx, r)
			if typ != "" {
				detectedCh <- typ
			}
		}(i)
	}

	wg.Wait()
	close(detectedCh)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cache.Done():
		// The cache can't be done until outputRdr is closed,
		// which obviously hasn't happened yet, so this cache
		// done scenario must be an error.
		if err = cache.Err(); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
	default:
	}

	var detectedTypes []string
	for typ := range detectedCh {
		if typ != "" {
			detectedTypes = append(detectedTypes, typ)
		}
	}

	if len(detectedTypes) == 0 {
		fmt.Fprintln(out, colorize(ansiRed, "typedetect: unable to detect type"))
	} else {
		fmt.Fprint(out,
			colorize(ansiGreen, "typedetect: "+strings.Join(detectedTypes, ", "))+"\n\n")
	}

	var lines int
	printSummary := func() {
		summary := fmt.Sprintf("\n%d lines [%d bytes]", lines, cache.Size())
		fmt.Fprintln(out, colorize(ansiGreen, summary))
	}

	var line string
	sc := bufio.NewScanner(previewRdr)
	for i := 0; i < numPreviewLines && sc.Scan(); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line = sc.Text()
		// Print the head of the input
		printPreviewLine(out, line)
		lines++
	}

	if err = sc.Err(); err != nil {
		return err
	}

	// Use a channel as a sliding window of lines; we're
	// only interested in the last printHeadTailLines lines.
	window := make(chan string, numPreviewLines)
	// Now gather the tail of the input.
	for sc.Scan() {
		line = sc.Text()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case window <- line:
		default:
			<-window
			window <- line
		}
		lines++
	}

	if sc.Err() != nil {
		return sc.Err()
	}

	var previewLines []string
	close(window)
	for line = range window {
		previewLines = append(previewLines, line)
	}

	if lines > len(previewLines)+numPreviewLines {
		fmt.Fprint(out, "\n"+colorize(ansiGreen, "[SNIP]")+"\n\n")
	}

	for _, line = range previewLines {
		printPreviewLine(out, line)
	}

	printSummary()
	return nil
}

func printPreviewLine(out io.Writer, line string) {
	if len(line) <= maxLineWidth {
		fmt.Fprintln(out, colorize(ansiFaint, line))
		return
	}

	fmt.Fprintln(out, colorize(ansiFaint, line[:maxLineWidth-3])+ellipsis)
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

func detectJSON(ctx context.Context, rc io.ReadCloser) (typ string) {
	defer rc.Close()

	dec := json.NewDecoder(rc)

	var err error
	for i := 0; i < tokenThreshold; i++ {
		select {
		case <-ctx.Done():
			return ""
		default:
		}
		if _, err = dec.Token(); err != nil {
			return ""
		}
	}

	return "json"
}

func detectXML(ctx context.Context, rc io.ReadCloser) (typ string) {
	defer rc.Close()

	dec := xml.NewDecoder(rc)
	var err error
	for i := 0; i < tokenThreshold; i++ {
		select {
		case <-ctx.Done():
			return ""
		default:
		}
		if _, err = dec.Token(); err != nil {
			return ""
		}
	}

	return "xml"
}

func detectHTML(ctx context.Context, rc io.ReadCloser) (typ string) {
	defer rc.Close()

	dec := html.NewTokenizer(rc)

	for i := 0; i < tokenThreshold; i++ {
		select {
		case <-ctx.Done():
			return ""
		default:
		}

		if dec.Next() == html.ErrorToken {
			return ""
		}
		_ = dec.Token()
		if dec.Err() != nil {
			return ""
		}
	}

	if dec.Err() != nil {
		return ""
	}

	return "html"
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
	ellipsis  = ansiGreen + "…" + ansiReset
)

func printErr(err error) {
	fmt.Fprintln(os.Stderr, colorize(ansiRed, "error: "+err.Error()))
}
