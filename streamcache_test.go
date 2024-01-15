package streamcache_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/errgroup"

	"github.com/neilotoole/streamcache"
)

const (
	numSampleRows = 1000000
	numG          = 10000
	jitterFactor  = 500
)

var _ io.ReadCloser = (*streamcache.ReadCloser)(nil)

func TestBasic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := generateSampleFile(t, numSampleRows)
	rcr := &readCloseRecorder{Reader: f}
	src := streamcache.NewSource(rcr)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	r, err := src.NewReader(ctx)
	require.NoError(t, err)

	src.Seal()

	defer func() {
		assert.NoError(t, r.Close())
		assert.Equal(t, 1, rcr.closed)
	}()

	gotB, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, wantB, gotB)
}

func TestConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	f := generateSampleFile(t, numSampleRows)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	rcr := &readCloseRecorder{Reader: bytes.NewReader(wantB)}
	src := streamcache.NewSource(rcr)
	require.NoError(t, err)

	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < numG; i++ {
		var r *streamcache.ReadCloser
		r, err = src.NewReader(gCtx)
		require.NoError(t, err)

		g.Go(func() error {
			defer func() {
				assert.NoError(t, r.Close())
			}()

			// Add some jitter
			time.Sleep(time.Nanosecond * time.Duration(rand.Intn(jitterFactor)))

			var gotB []byte
			gotB, err = io.ReadAll(r)
			if err != nil {
				assert.NoError(t, r.Close())
				return err
			}

			assert.Equal(t, wantB, gotB)
			return nil
		})
	}

	src.Seal()

	err = g.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 1, rcr.closed)
}

func TestSeal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := streamcache.NewSource(strings.NewReader("anything"))
	r, err := src.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r)

	src.Seal()

	r, err = src.NewReader(ctx)
	require.Error(t, err)
	require.Equal(t, streamcache.ErrSealed, err)
	require.Nil(t, r)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	f := generateSampleFile(t, numSampleRows)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	rcr := &readCloseRecorder{Reader: f}
	src := streamcache.NewSource(rcr)

	r1, err := src.NewReader(ctx)
	require.NoError(t, err)

	gotB1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, rcr.closed)

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)
	src.Seal()

	gotB2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB2)
	require.NoError(t, r2.Close())
	require.Equal(t, 1, rcr.closed)
}

// generateSampleFile generates a temp file of sample data with the
// specified number of rows. It is the caller's responsibility to
// close the file. Note that the file is removed by t.Cleanup.
func generateSampleFile(t *testing.T, rows int) *os.File {
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(f.Name()))
	})

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z\n"
	for i := 0; i < rows; i++ {
		// Actual data lines will look like:
		//  0,A,B,C...
		//  1,A,B,C...
		_, err = f.WriteString(strconv.Itoa(i) + "," + line)
		require.NoError(t, err)
	}

	return f
}

// readCloseRecorder is used to verify that Close was invoked an
// expected number of times.
type readCloseRecorder struct {
	mu sync.Mutex
	io.Reader
	closed int
}

// Close implements io.Close, and increments its closed field each
// time that Close is invoked.
func (rc *readCloseRecorder) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if c, ok := rc.Reader.(io.ReadCloser); ok {
		err := c.Close()
		rc.closed++
		return err
	}

	rc.closed++
	return nil
}

func TestContextAwareness(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("oh noes")
	originRdr := DelayReader(LimitRandReader(100000), time.Second, true)
	src := streamcache.NewSource(originRdr)

	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)
	time.AfterFunc(time.Second, func() {
		cancel(wantErr)
	})

	r, err := src.NewReader(ctx)
	require.NoError(t, err)
	_, gotErr := io.ReadAll(r)
	require.Error(t, gotErr)
	require.True(t, errors.Is(gotErr, wantErr))
}

func TestErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("oh noes")
	const errAfterN = 50

	originRdr := NewErrorAfterNReader(errAfterN, wantErr)
	src := streamcache.NewSource(originRdr)

	r1, err := src.NewReader(ctx)
	require.NoError(t, err)
	b1, err := io.ReadAll(r1)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(b1))

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)
	b2, err := io.ReadAll(r2)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(b2))
}
