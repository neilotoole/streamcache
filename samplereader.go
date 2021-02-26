// Package samplereader implements a reader mechanism that allows
// multiple callers to sample some or the entire contents of a
// source reader, while only reading from the source reader once.
//
// This is admittedly a rather arcane need.
// Let's say we're reading from stdin. For example:
//
//  # cat myfile.ext | myprogram
//
// In this scenario, myprogram wants to detect the type of data
// in the file/pipe, and then print it out. The input file could be,
// let's say, a CSV file, or a TSV file.
//
// The obvious approach is to inspect the first few lines of the
// input, and check if the input is either valid CSV, or valid TSV.
// After that process, let's say we want to dump out the entire contents
// of the input.
//
// Package samplereader provides a facility to create a Source from an
// underlying io.Reader (os.Stdin in this scenario), and spawn multiple
// readers, each of which can operate independently, in their own
// goroutines if desired. The underlying source (again, os.Stdin in this
// scenario) will only be read from once, but its data is available to
// multiple readers, because that data is cached in memory.
// That is, until there's only one final reader left, (after invoking
// Source.Seal) at which point the cache is discarded, and
// the final reader reads straight from the underlying source.
package samplereader

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// ErrSealed is returned by Source.NewReadCloser if the source is
// already sealed.
var ErrSealed = errors.New("already sealed")

// Source encapsulates an underlying io.Reader that many callers
// can read from.
type Source struct {
	src    io.Reader
	mu     sync.Mutex
	sealed bool
	count  int
	buf    *buffer
}

// NewSource returns a new source with r as the underlying reader.
func NewSource(r io.Reader) *Source {
	return &Source{src: r, buf: &buffer{}}
}

// NewReadCloser returns a new ReadCloser for Source. It is the caller's
// responsibility to close the returned ReadCloser.
func (s *Source) NewReadCloser() (*ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil, ErrSealed
	}

	s.count++
	return &ReadCloser{s: s}, nil
}

func (s *Source) readAt(p []byte, offset int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := int(offset) + len(p)
	bufLen := len(s.buf.b)
	if end < bufLen {
		// We already have the data in the buf cache
		return copy(p, s.buf.b[offset:int(offset)+len(p)]), nil
	}

	need := end - bufLen
	tmp := make([]byte, need)
	n, err = s.src.Read(tmp)

	if n > 0 {
		_, _ = s.buf.Write(tmp[0:n]) // can't error
	}

	bufLen = len(s.buf.b)

	if int(offset) >= bufLen {
		return 0, err
	}

	if end > bufLen {
		end = bufLen
	}

	return copy(p, s.buf.b[offset:end]), err
}

func (s *Source) close(rc *ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.count--
	if !s.sealed {
		return nil
	}

	switch s.count {
	default:
		// There's still more than one open reader instances, so we
		// don't close the underlying reader.
		return nil
	case 0:
		if c, ok := s.src.(io.Closer); ok {
			return c.Close()
		}
		return nil
	case 1:
		// Continues below
	}

	// Now there's only one final reader left, so we switch
	// to "direct mode" for that final reader.
	// The bytes from buf are the "penultimate" bytes, because the
	// "ultimate" bytes come from s.src itself.
	penultimateBytes := s.buf.b[rc.offset:]

	// Safe to modify rc.finalReadCloser because rc calls this close()
	// method already being inside its own mu.Lock.
	rc.finalReadCloser = &finalReadCloser{
		Reader: io.MultiReader(bytes.NewReader(penultimateBytes), s.src),
		src:    s.src,
	}

	return nil
}

// Seal is called to indicate that there will be no more calls
// to NewReadCloser.
func (s *Source) Seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

// ReadCloser is returned by Source.NewReadCloser. It is the
// responsibility of the receiver to close the ReadCloser.
type ReadCloser struct {
	mu        sync.Mutex
	s         *Source
	offset    int
	closeOnce sync.Once
	closeErr  error

	// finalReadCloser is only set if this ReadCloser becomes the
	// last-man-standing of the parent Source's spawned ReadCloser
	// children. If finalReadCloser is set, then finalReadCloser will be used
	// by Read to read out the rest of data from the parent Source's
	// underlying Reader.
	finalReadCloser io.Reader
}

// Read implements io.Reader.
func (rc *ReadCloser) Read(p []byte) (n int, err error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.finalReadCloser == nil {
		// We're not in "last-man-standing" mode, so just continue
		// as normal.
		n, err = rc.s.readAt(p, int64(rc.offset))
		rc.offset += n
		return n, err
	}

	// This ReadCloser is in "last-man-standing" mode, so we switch
	// to "direct mode".
	return rc.finalReadCloser.Read(p)
}

// Close closes this ReadCloser. If the parent Source is not sealed,
// this method is effectively a no-op. If the parent Source is sealed
// and this is the last remaining reader, the parent Source's underlying
// io.Reader is closed (if it implements io.Closer), and this ReadCloser
// switches to "direct mode" reading for the remaining data.
// Note that subsequent calls to this method are no-op and return the
// same result as the first call.
func (rc *ReadCloser) Close() error {
	rc.closeOnce.Do(func() {
		rc.closeErr = rc.s.close(rc)
	})

	return rc.closeErr
}

// finalReadCloser is used by the final child of Source.NewReader
// to wrap up the reading/closing without the use of the cache.
type finalReadCloser struct {
	io.Reader
	src io.Reader
}

// Close implements io.Closer.
func (fc *finalReadCloser) Close() error {
	if c, ok := fc.src.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

// buffer is a basic implementation of io.Writer.
// FIXME: are we using buffer?
type buffer struct {
	b []byte
}

// Write implements io.Writer.
func (b *buffer) Write(p []byte) (n int, err error) {
	b.b = append(b.b, p...)
	return len(p), nil
}
