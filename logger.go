package streamcache

// logger.go contains temporary logging functionality
// during development. It will be removed before release.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var shouldLog bool

func init() { //nolint:gochecknoinits
	shouldLog, _ = strconv.ParseBool(os.Getenv("STREAMCACHE_LOG"))
}

const (
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
	ansiCyan  = "\033[36m"
	ansiWhite = "\033[37m"
)

// logf prints a log message to stdout if the STREAMCACHE_LOG envar is true.
// This function will be removed before release.
func logf(r *Reader, format string, args ...any) {
	if !shouldLog {
		return
	}

	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	fs := runtime.CallersFrames([]uintptr{pcs[0]})
	f, _ := fs.Next()
	if f.File == "" {
		return
	}

	color := ansiCyan
	switch {
	case strings.Contains(r.Name, "red"):
		color = ansiRed
	case strings.Contains(r.Name, "green"):
		color = ansiGreen
	case strings.Contains(r.Name, "blue"):
		color = ansiBlue
	default:
	}

	s := filepath.Base(f.File) + ":" + strconv.Itoa(f.Line) +
		"   " + color + fmt.Sprintf("%13s", r.Name) + ansiReset +
		"    " + fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stdout, s)
}
