package streamcache

import (
	"context"
	"sync"
)

// Reader is returned by Source.NewReader. It is the
// responsibility of the receiver to close the returned Reader.
type Reader struct {
	mu  sync.Mutex
	ctx context.Context
	s   *Source

	// readFn is the func that Reader.Read calls to read bytes.
	// Initially it is set to Source.readAtFn, but if this reader
	// becomes the last man standing, this field may be set
	// to Source.readDirect.
	readFn  readAtFunc
	offset  int
	readErr error

	closeOnce sync.Once
	closeErr  error
	closed    bool
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return 0, context.Cause(r.ctx)
		default:
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.readErr != nil {
		return 0, r.readErr
	}

	if r.closed {
		return 0, ErrAlreadyClosed
	}

	n, err = r.readFn(r, p, r.offset)
	r.readErr = err
	r.offset += n
	return n, err
}

// Close closes this Reader. If the parent Source is not sealed,
// this method is effectively a no-op. If the parent Source is sealed
// and this is the last remaining reader, the parent Source's underlying
// io.Reader is closed (if it implements io.Closer).
//
// Note that subsequent calls to this method are no-op and return the
// same result as the first call.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.closed = true
		r.closeErr = r.s.close(r)
	})

	return r.closeErr
}

type readAtFunc func(r *Reader, p []byte, offset int) (n int, err error)
