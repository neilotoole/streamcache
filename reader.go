package streamcache

import (
	"context"
	"sync"
)

// Reader is returned by Cache.NewReader. It is the
// responsibility of the receiver to close the returned Reader.
type Reader struct {
	Name string

	// mu guards Reader's methods.
	mu sync.Mutex

	// ctx is the context provided to Cache.NewReader. If non-nil,
	// every invocation of Reader.Read checks ctx for cancellation,
	// before proceeding. Note that Reader.Close ignores ctx.
	ctx context.Context

	// c is the Reader's parent Cache.
	c *Cache

	// readFn is the func that Reader.Read invokes to read bytes.
	// Initially it is set to Cache.read, but if this reader
	// becomes the last-man-standing, this field may be set
	// to Cache.readOriginDirect.
	readFn readFunc

	// offset is the offset into the stream from which the next
	// Read will read. It is incremented by each Read.
	offset int

	// readErr is set by Reader.Read when an error is returned
	// from the source, and its non-nil value is returned by
	// subsequent calls to Read. That is to say: the same non-nil
	// read error is returned every time.
	readErr error

	// pCloseErr is set by Reader.Close, and the set value is
	// returned by subsequent calls to Close.
	pCloseErr *error
}

// Read implements io.Reader. If the non-nil context provided to
// Cache.NewReader is canceled, Read will return the context's error
// via context.Cause. If this reader has already been closed via Reader.Close,
// Read will return ErrAlreadyClosed. If a previous invocation of Read returned
// an error from the origin reader, that error is returned. Otherwise Read
// reads from Cache, which may return bytes from Cache's cache or new bytes
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

// Close closes this Reader. If the parent Cache is not sealed,
// this method is effectively a no-op. If the parent Cache is sealed
// and this is the last remaining reader, the Cache's origin io.Reader is
// closed, if it implements io.Closer. At that point, the Cache is
// considered done, and the channel returned by Cache.Done is closed.
//
// The Close operation proceeds even if the non-nil context provided
// to Cache.NewReader is cancelled. That is to say, Close ignores context.
//
// Note that subsequent calls to Close are no-op and return the same result
// as the first call.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pCloseErr != nil {
		// Already closed. Return the same error as the first call
		// to Close (which may be nil).
		return *r.pCloseErr
	}

	closeErr := r.c.close(r)
	r.pCloseErr = &closeErr
	return closeErr
}
