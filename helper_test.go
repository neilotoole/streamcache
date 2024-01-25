package streamcache_test

// File helper_test.go contains test helper functionality.

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/neilotoole/streamcache"
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

var _ io.Reader = (*tweakableReader)(nil)

// tweakableReader is an io.Reader that can be configured
// by test code. Each call to Read consults the reader's fields anew.
// It is typical for test code to manipulate those fields between
// calls to Read. Note that if the unblock channel field is non-nil,
// it must be closed or have a value sent on it for Read to proceed.
type tweakableReader struct {
	unblock chan struct{}
	err     error
	data    []byte
}

// Read implements io.Reader. Each call to Read consults the
// reader's fields.
func (r *tweakableReader) Read(p []byte) (n int, err error) {
	if r.unblock != nil {
		<-r.unblock
	}
	n = copy(p, r.data)
	return n, r.err
}

// requireNoTake fails if a value is taken from c.
func requireNoTake[C any](tb testing.TB, c <-chan C, msgAndArgs ...any) {
	tb.Helper()
	select {
	case <-c:
		require.Fail(tb, "unexpected take from channel", msgAndArgs...)
	default:
	}
}

// requireTake fails if a value is not taken from c.
func requireTake[C any](tb testing.TB, c <-chan C, msgAndArgs ...any) {
	tb.Helper()
	select {
	case <-c:
	default:
		require.Fail(tb, "unexpected failure to take from channel", msgAndArgs...)
	}
}

// totalTimeout is used by requireTotal and requireNoTotal.
const totalTimeout = time.Millisecond * 100

// requireNoTotal requires that s.Total blocks.
func requireNoTotal(tb testing.TB, s *streamcache.Stream) {
	tb.Helper()

	failErr := errors.New("fail")
	ctx, cancel := context.WithCancelCause(context.Background())

	var (
		size int
		err  error
		wait = make(chan struct{})
	)

	go func() {
		time.AfterFunc(totalTimeout, func() {
			cancel(failErr)
		})
		size, err = s.Total(ctx)
		wait <- struct{}{}
	}()

	<-wait
	require.Error(tb, err)
	require.True(tb, errors.Is(err, failErr))
	require.Equal(tb, 0, size)
}

// requireTotal requires that s.Total doesn't block, and
// that s.Total returns want and no error.
func requireTotal(tb testing.TB, s *streamcache.Stream, want int) {
	tb.Helper()

	var (
		ctx, cancel = context.WithCancelCause(context.Background())
		err         error
		size        int
		wait        = make(chan struct{})
	)

	go func() {
		time.AfterFunc(totalTimeout, func() {
			cancel(errors.New("fail"))
		})
		size, err = s.Total(ctx)
		wait <- struct{}{}
	}()

	<-wait
	require.NoError(tb, err)
	require.Equal(tb, want, size)
}

// generateSampleFile generates a temp file of sample data with the
// specified number of rows. It is the caller's responsibility to
// close the file. Note that the file is removed by t.Cleanup.
func generateSampleFile(tb testing.TB, rows int) (size int, fp string) {
	f, err := os.CreateTemp("", "")
	require.NoError(tb, err)
	fp = f.Name()

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z"
	for i := 0; i < rows; i++ {
		// Actual data lines will look like:
		//  0,A,B,C...
		//  1,A,B,C...
		s := strconv.Itoa(i) + "," + line
		_, err = fmt.Fprintln(f, s)
		require.NoError(tb, err)
	}

	require.NoError(tb, f.Close())
	fi, err := os.Stat(fp)
	require.NoError(tb, err)
	size = int(fi.Size())
	tb.Logf("Generated sample file [%d]: %s", size, fp)
	return int(fi.Size()), fp
}

func sleepJitter() {
	const jitterFactor = 30
	d := time.Millisecond * time.Duration(mrand.Intn(jitterFactor))
	time.Sleep(d)
}
