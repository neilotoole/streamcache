package streamcache

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const (
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
	ansiCyan  = "\033[36m"
)

func (c *Cache) Infof(r *Reader, format string, args ...any) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, Infof]

	fs := runtime.CallersFrames([]uintptr{pcs[0]})
	f, _ := fs.Next()
	if f.File == "" {
		return
	}
	color := ansiCyan
	switch {
	case strings.Contains(r.Name, "green"):
		color = ansiGreen
	case strings.Contains(r.Name, "blue"):
		color = ansiBlue
	case strings.Contains(r.Name, "red"):
		color = ansiRed
	default:
	}

	s := filepath.Base(f.File) + ":" + strconv.Itoa(f.Line) +
		"   " + color + fmt.Sprintf("%13s", r.Name) + ansiReset +
		"    " + fmt.Sprintf(format, args...)
	// println(s)
	_ = s
	// fmt.Fprintln(os.Stdout, s)
}
