package streamcache_test

// File helper_test.go contains test helper functionality.

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"sync"
	"testing"
	"time"

	"github.com/neilotoole/streamcache"
	"github.com/stretchr/testify/require"
)

var _ io.Reader = (*delayReader)(nil)

// newDelayReader returns an io.Reader that delays on each read from r.
// If jitter is true, a randomized jitter factor is added to the delay.
// If r implements io.Closer, the returned reader will also
// implement io.Closer; if r doesn't implement io.Closer,
// the returned reader will not implement io.Closer.
// If r is nil, nil is returned.
func newDelayReader(r io.Reader, delay time.Duration, jitter bool) io.Reader {
	if r == nil {
		return nil
	}

	dr := delayReader{r: r, delay: delay, jitter: jitter}
	if _, ok := r.(io.Closer); ok {
		return delayReadCloser{dr}
	}
	return dr
}

var _ io.Reader = (*delayReader)(nil)

type delayReader struct {
	r      io.Reader
	delay  time.Duration
	jitter bool
}

// Read implements io.Reader.
func (d delayReader) Read(p []byte) (n int, err error) {
	delay := d.delay
	if d.jitter {
		delay += time.Duration(mrand.Int63n(int64(d.delay))) / 3
	}

	time.Sleep(delay)
	return d.r.Read(p)
}

type delayReadCloser struct {
	delayReader
}

// Close implements io.Closer.
func (d delayReadCloser) Close() error {
	if c, ok := d.r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// newErrorAfterNReader returns an io.Reader that returns err after
// reading n random bytes from crypto/rand.Reader.
func newErrorAfterNReader(n int, err error) io.Reader {
	return &errorAfterNReader{afterN: n, err: err}
}

type errorAfterNReader struct {
	err    error
	mu     sync.Mutex
	afterN int
	count  int
}

func (r *errorAfterNReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count >= r.afterN {
		return 0, r.err
	}

	// There's some bytes to read
	allowed := r.afterN - r.count
	if allowed > len(p) {
		n, _ = crand.Read(p)
		r.count += n
		return n, nil
	}
	n, _ = crand.Read(p[:allowed])
	if n != allowed {
		panic(fmt.Sprintf("expected to readMain %d bytes, got %d", allowed, n))
	}
	r.count += n
	return n, r.err
}

// newLimitRandReader returns an io.Reader that reads up to limit bytes
// from crypto/rand.Reader.
func newLimitRandReader(limit int64) io.Reader {
	return io.LimitReader(crand.Reader, limit)
}

var _ io.Reader = (*rcRecorder)(nil)

// rcRecorder wraps an io.Reader and records stats.
type rcRecorder struct {
	r          io.Reader
	closeCount int
	size       int
	mu         sync.Mutex
}

func (rc *rcRecorder) Read(p []byte) (n int, err error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	n, err = rc.r.Read(p)
	rc.size += n
	return n, err
}

// Close implements io.Close, and increments its closed field each
// time that Close is invoked.
func (rc *rcRecorder) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.closeCount++
	if c, ok := rc.r.(io.ReadCloser); ok {
		return c.Close()
	}

	return nil
}

func sleepJitter() {
	const jitterFactor = 30
	d := time.Millisecond * time.Duration(mrand.Intn(jitterFactor))
	time.Sleep(d)
}

func requireNoTake[C any](t *testing.T, c <-chan C, msgAndArgs ...any) {
	t.Helper()
	select {
	case <-c:
		require.Fail(t, "unexpected take from channel", msgAndArgs...)
	default:
	}
}

func requireTake[C any](t *testing.T, c <-chan C, msgAndArgs ...any) {
	t.Helper()
	select {
	case <-c:
	default:
		require.Fail(t, "unexpected failure to take from channel", msgAndArgs...)
	}
}

// requireNoTotal requires that s.Total blocks.
func requireNoTotal(t *testing.T, s *streamcache.Stream) {
	t.Helper()

	failErr := errors.New("fail")
	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(time.Millisecond*10, func() {
		cancel(failErr)
	})

	var (
		size int
		err  error
		wait = make(chan struct{})
	)

	go func() {
		size, err = s.Total(ctx)
		wait <- struct{}{}
	}()

	<-wait
	require.Error(t, err)
	require.True(t, errors.Is(err, failErr))
	require.Equal(t, 0, size)
}

// requireTotal requires that s.Total doesn't block, and
// returns want and no error.
func requireTotal(t *testing.T, s *streamcache.Stream, want int) {
	t.Helper()

	ctx, cancel := context.WithCancelCause(context.Background())
	time.AfterFunc(time.Millisecond*10, func() {
		cancel(errors.New("fail"))
	})

	var (
		err  error
		size int
		wait = make(chan struct{})
	)
	go func() {
		size, err = s.Total(ctx)
		wait <- struct{}{}
	}()

	<-wait
	require.NoError(t, err)
	require.Equal(t, want, size)
}

// FIXME: delete enableLogging.
func enableLogging(t *testing.T) { //nolint:unused
	t.Setenv("STREAMCACHE_LOG", "true")
}
