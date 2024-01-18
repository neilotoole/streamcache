package streamcache

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"

	"github.com/neilotoole/sq/libsq/core/lg"
)

func (c *Cache) getLog(r *Reader) *slog.Logger {
	if c.log == nil {
		return lg.Discard()
	}
	return c.log.With("rdr", r.Name)
}

//func (c *Cache) Infof(l *slog.Logger, lvl slog.Level, format string, args ...any) {
//	var pcs [1]uintptr
//	runtime.Callers(3, pcs[:]) // skip [Callers, Infof]
//	r := slog.NewRecord(time.Now(), lvl, fmt.Sprintf(format, args...), pcs[0])
//	_ = r
//	_ = l.Handler().Handle(context.Background(), r)
//}

const (
	ansiReset = "\033[0m"
	ansiRed   = "\033[31m"
	ansiGreen = "\033[32m"
	ansiBlue  = "\033[34m"
	ansiCyan  = "\033[36m"
)

func (c *Cache) Infof(r *Reader, l *slog.Logger, lvl slog.Level, format string, args ...any) {
	c.logMu.Lock()
	c.logMu.Unlock()
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, Infof]

	fs := runtime.CallersFrames([]uintptr{pcs[0]})
	f, _ := fs.Next()
	if f.File == "" {
		return
	}
	// dir, file := filepath.Split(f.File)
	// fn := f.Function

	//parts := strings.Split(f.Function, "/")
	//if len(parts) > 0 {
	//	fn = parts[len(parts)-1]
	//}
	color := ansiCyan
	switch r.Name {
	case "green-regular":
		color = ansiGreen
	case "blue-lower":
		color = ansiBlue
	case "red-upper":
		color = ansiRed
	}

	s := filepath.Base(f.File) + ":" + strconv.Itoa(f.Line) +
		"   " + color + fmt.Sprintf("%13s", r.Name) + ansiReset +
		"    " + fmt.Sprintf(format, args...)
	// println(s)
	_ = s
	// fmt.Fprintln(os.Stdout, s)
}
