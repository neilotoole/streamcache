// Package main provides the "typedetect" example CLI that detects the type
// of a data file, e.g. JSON, XML, etc. Usage:
//
//	$ typedetect FILE
//	$ cat FILE | typedetect
//
// The tool prints the detected type, and a preview of the
// file contents.
//
// "typedetect" exists to demonstrate use of neilotoole/streamcache.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"

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

	// Determine if input is coming from stdin (`cat FILE | typedetect`),
	// or via args (`typedetect FILE`).

	var fi os.FileInfo
	if fi, err = os.Stdin.Stat(); err != nil {
		printErr(err)
		return
	}

	var in *os.File
	if os.ModeNamedPipe&fi.Mode() > 0 || fi.Size() > 0 {
		// Input is from stdin.
		if len(os.Args) > 1 {
			// If input is from stdin, then we don't want any args.
			// - `cat FILE | typedetect` is ok, but...
			// - `cat FILE | typedetect FILE` is not.
			err = errUsage
			printErr(err)
			return
		}
		in = os.Stdin
	} else {
		// Input is from args, e.g. `typedetect FILE`.
		if len(os.Args) != 2 || (os.Args[1] == "") {
			err = errUsage
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

	// The CLI is set up, now we can get on with the work.
	if err = exec(ctx, in, os.Stdout); err != nil {
		printErr(err)
	}
}

// exec does the actual work of typedetect.
func exec(ctx context.Context, in io.Reader, out io.Writer) error {
	detectors := []detectFunc{detectJSON, detectXML}

	stream := streamcache.New(in)
	rdrs := make([]*streamcache.Reader, len(detectors))
	for i := range detectors {
		rdrs[i] = stream.NewReader(ctx)
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

	// In theory multiple detectors could succeed, so we
	// gather all the results and print them.
	close(detectionCh)
	var types []string
	for typ := range detectionCh {
		types = append(types, typ)
	}

	if len(types) == 0 {
		fmt.Fprintln(out, colorize(ansiRed, "typedetect: unable to detect type"))
		// Even if we can't detect the type, we still continue below
		// to print the head and tail preview.
	} else {
		fmt.Fprint(out, colorize(ansiGreen, "typedetect: "+strings.Join(types, ", "))+"\n")
	}

	// previewRdr reads the content, from which we print the head
	// and tail, each up to numPreviewLines lines.
	previewRdr := stream.NewReader(ctx)
	defer previewRdr.Close()

	// There will be no new readers after this point, so we can
	// seal the stream. This results in previewRdr switching to
	// reading directly from the source reader, as soon as it has
	// exhausted the stream's cache. This mode switch is internal
	// to streamcache; the caller knows nothing of it.
	stream.Seal()

	// Scan and print up to numPreviewLines from input head.
	var lineCount int
	sc := bufio.NewScanner(previewRdr)
	for i := 0; i < numPreviewLines && sc.Scan(); i++ {
		select {
		// Offer a chance to bail out early on Ctrl-C.
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		printPreviewLine(out, sc.Text())
		lineCount++
	}

	if err := sc.Err(); err != nil {
		return err
	}

	// Use a channel as a sliding window / circular buffer of input lines
	// so that, at the end, we can print the final numPreviewLines of
	// the tail, and just skip the stuff in the middle.
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

	if err := sc.Err(); err != nil {
		return err
	}

	// We're done with processing the input now. We can
	// close the window.
	close(window)

	skipCount := lineCount - len(window) - numPreviewLines
	if skipCount > 0 {
		skipMsg := fmt.Sprintf("[Skipped %d lines]", skipCount)
		fmt.Fprintln(out, colorize(ansiGreen, skipMsg))
	}

	for line = range window {
		printPreviewLine(out, line)
	}

	summary := fmt.Sprintf("%d lines (%d bytes)", lineCount, stream.Size())
	fmt.Fprintln(out, colorize(ansiGreen, summary))
	return nil
}

// detectFunc is a function that detects the type of data on rc.
// On success, the function returns a non-empty string, e.g. "json"
// or "xml". On failure, the function returns empty string. The
// function must close rc in either case.
type detectFunc func(ctx context.Context, rc io.ReadCloser) (typ string)

// detectJSON returns "json" if rc appears to contain JSON, otherwise
// it returns empty string. It closes rc in either case.
func detectJSON(ctx context.Context, rc io.ReadCloser) (typ string) {
	defer rc.Close()

	dec := json.NewDecoder(rc)
	var err error
	for i := 0; i < tokenDetectThreshold; i++ {
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
	for i := 0; i < tokenDetectThreshold; i++ {
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
	// tokenDetectThreshold is the number of tokens to be read
	// successfully before we consider the type to be detected.
	tokenDetectThreshold = 10

	// numPreviewLines is the number of preview lines to print from the
	// head and tail of the input. So, the total number of lines printed
	// is numPreviewLines*2.
	numPreviewLines = 5

	// maxPreviewLineWidth is the width at which a preview line
	// is truncate before printing.
	maxPreviewLineWidth = 80

	// terminal colors.
	ansiReset = "\033[0m"
	ansiFaint = "\033[2m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
)

// printPreviewLine prints a line of the input to out. Long lines are
// truncated at maxPreviewLineWidth and have an ellipsis added.
func printPreviewLine(out io.Writer, line string) {
	const ellipsis = ansiGreen + "…" + ansiReset

	if len(line) <= maxPreviewLineWidth {
		fmt.Fprintln(out, colorize(ansiFaint, line))
		return
	}

	fmt.Fprintln(out, colorize(ansiFaint, line[:maxPreviewLineWidth-3])+ellipsis)
}

func colorize(ansi, s string) string {
	return ansi + s + ansiReset
}

func printErr(err error) {
	fmt.Fprintln(os.Stderr, colorize(ansiRed, "typedetect: error: "+err.Error()))
}

var errUsage = errors.New("usage: `typedetect FILE` or `cat FILE | typedetect`")
