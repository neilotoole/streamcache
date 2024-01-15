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
	// to Source.readOriginDirect.
	readFn  readFunc
	offset  int
	readErr error

	pCloseErr *error

	// closeOnce sync.Once
	// closeErr  error
	// closed    bool
}

// Read implements io.Reader. If the non-nil context provided to
// Source.NewReader is canceled, Read will return the context's error
// via context.Cause. If this reader has already been closed via Reader.Close,
// Read will return ErrAlreadyClosed. If a previous invocation of Read returned
// an error from the origin reader, that error is returned. Otherwise Read
// reads from Source, which may return bytes from Source's cache or new bytes
// from origin, or a combination of both.
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

	if r.pCloseErr != nil {
		return 0, ErrAlreadyClosed
	}

	n, err = r.readFn(r, p, r.offset)
	r.readErr = err
	r.offset += n
	return n, err
}

// Close closes this Reader. If the parent Source is not sealed,
// this method is effectively a no-op. If the parent Source is sealed
// and this is the last remaining reader, the Source's origin io.Reader is
// closed, if it implements io.Closer. At that point, the Source is
// considered done, and the channel returned by Source.Done is closed.
//
// Note that subsequent calls to this method are no-op and return the
// same result as the first call.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pCloseErr != nil {
		// Already closed. Return the same error as the first call
		// to Close (which may be nil).
		return *r.pCloseErr
	}

	closeErr := r.s.close(r)
	r.pCloseErr = &closeErr
	return closeErr
}
