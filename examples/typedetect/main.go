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

	// Determine if input is coming from stdin (e.g. `cat FILE | typedetect`),
	// or via args (e.g. `typedetect FILE`).
	var in *os.File
	fi, err := os.Stdin.Stat()
	if err != nil {
		printErr(err)
		return
	}

	if os.ModeNamedPipe&fi.Mode() > 0 || fi.Size() > 0 {
		if len(os.Args) > 1 {
			err = usageErr
			printErr(err)
			return
		}
		in = os.Stdin
	} else {
		if len(os.Args) != 2 || (os.Args[1] == "") {
			err = usageErr
			printErr(err)
			return
		}
		in, err = os.Open(os.Args[1])
		if err != nil {
			printErr(err)
			return
		}
		defer in.Close()
	}

	if err = exec(ctx, log, in, os.Stdout); err != nil {
		printErr(err)
	}
}

// detectFunc is a function that detects the type of data on rc.
// On success, the function returns a non-empty string, e.g. "json"
// or "xml". On failure, the function returns empty string. The
// function must close rc in either case.
type detectFunc func(ctx context.Context, rc io.ReadCloser) (typ string)

func exec(ctx context.Context, log *slog.Logger, in io.Reader, out io.Writer) error {
	detectors := []detectFunc{detectJSON, detectXML}

	cache := streamcache.New(in)
	rdrs := make([]*streamcache.Reader, len(detectors))
	var err error
	for i := range detectors {
		if rdrs[i], err = cache.NewReader(ctx); err != nil {
			return err
		}
	}

	detectionCh := make(chan string, len(detectors))
	wg := &sync.WaitGroup{}
	wg.Add(len(detectors))
	for i := range detectors {
		go func(i int) {
			defer wg.Done()
			r, detector := rdrs[i], detectors[i]
			defer r.Close()

			if typ := detector(ctx, r); typ != "" {
				detectionCh <- typ
			}
		}(i)
	}

	wg.Wait()
	close(detectionCh)

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

	// In theory multiple detectors could succeed, so we
	// gather all the results and print them.
	var detectedTypes []string
	for typ := range detectionCh {
		detectedTypes = append(detectedTypes, typ)
	}

	if len(detectedTypes) == 0 {
		fmt.Fprintln(out, colorize(ansiRed, "typedetect: unable to detect type"))
		// Even if we can't detect the type, we still continue below
		// tp print the head and tail preview.
	} else {
		fmt.Fprint(out,
			colorize(ansiGreen, "typedetect: "+strings.Join(detectedTypes, ", "))+"\n")
	}

	// previewRdr reads the content, prints the head and tail, each
	// up to numPreviewLines lines.
	previewRdr, err := cache.NewReader(ctx)
	if err != nil {
		return err
	}
	defer previewRdr.Close()

	// There will be no new readers after this point, so we can
	// seal the cache. This results in previewRdr switching to
	// reading directly from the source reader, as soon as it
	// has exhausted the cache. This mode switch is transparent to
	// the caller here of course; streamcache takes care of it.
	if err = cache.Seal(); err != nil {
		return err
	}
	// Scan and print up to numPreviewLines from input head.
	var lineCount int
	sc := bufio.NewScanner(previewRdr)
	for i := 0; i < numPreviewLines && sc.Scan(); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		printPreviewLine(out, sc.Text())
		lineCount++
	}

	if err = sc.Err(); err != nil {
		return err
	}

	// Use a channel as a sliding window / circular buffer of lines
	// so that, at the end, we can print the tail of numPreviewLines,
	// and just skip the stuff in the middle.
	window := make(chan string, numPreviewLines)
	var line string
	for sc.Scan() {
		line = sc.Text()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case window <- line:
			// The window is not yet full, so just add the line.
		default:
			// The window is full, so pop the oldest line.
			<-window
			// And now add the new line.
			window <- line
		}
		lineCount++
	}

	if sc.Err() != nil {
		return sc.Err()
	}

	// We're done with processing the input now. We can
	// close the window.
	close(window)

	skipCount := lineCount - len(window) - numPreviewLines
	if skipCount > 0 {
		fmt.Fprintln(out, colorize(ansiGreen, fmt.Sprintf("Skip %d line(s)", skipCount)+ellipsis))
	}

	for line = range window {
		printPreviewLine(out, line)
	}

	summary := fmt.Sprintf("%d lines [%d bytes]", lineCount, cache.Size())
	fmt.Fprintln(out, colorize(ansiGreen, summary))
	return nil
}

// printPreviewLine prints a line of the input. It
// truncates long lines at maxLineWidth and adds
// an ellipsis...
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

// detectJSON returns "json" if rc appears to contain JSON, otherwise
// it returns empty string. It closes rc in either case.
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

// detectXML returns "xml" if rc appears to contain XML, otherwise
// it returns empty string. It closes rc in either case.
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

const (
	tokenThreshold  = 10
	numPreviewLines = 5
	maxLineWidth    = 80
	ansiReset       = "\033[0m" // terminal colors
	ansiFaint       = "\033[2m"
	ansiRed         = "\033[31m"
	ansiGreen       = "\033[32m"
	ellipsis        = ansiGreen + "…" + ansiReset
)

func colorize(ansi, s string) string {
	return ansi + s + ansiReset
}

func printErr(err error) {
	fmt.Fprintln(os.Stderr, colorize(ansiRed, "typedetect: error: "+err.Error()))
}

var usageErr = errors.New("usage: `typedetect FILE` or `cat FILE | typedetect`")
