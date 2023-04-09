package streamcache_test

import (
	"bytes"
	"io"
	"io/ioutil"
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
	f := generateSampleFile(t, numSampleRows)
	rcr := &readCloseRecorder{Reader: f}
	src := streamcache.NewSource(rcr)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	r, err := src.NewReadCloser()
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
	f := generateSampleFile(t, numSampleRows)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	rcr := &readCloseRecorder{Reader: bytes.NewReader(wantB)}
	src := streamcache.NewSource(rcr)
	require.NoError(t, err)

	g := &errgroup.Group{}
	for i := 0; i < numG; i++ {
		r, err := src.NewReadCloser()
		require.NoError(t, err)

		g.Go(func() error {
			defer func() {
				assert.NoError(t, r.Close())
			}()

			// Add some jitter
			time.Sleep(time.Nanosecond * time.Duration(rand.Intn(jitterFactor)))

			gotB, err := io.ReadAll(r)
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
	src := streamcache.NewSource(strings.NewReader("anything"))
	r, err := src.NewReadCloser()
	require.NoError(t, err)
	require.NotNil(t, r)

	src.Seal()

	r, err = src.NewReadCloser()
	require.Error(t, err)
	require.Equal(t, streamcache.ErrSealed, err)
	require.Nil(t, r)
}

func TestClose(t *testing.T) {
	f := generateSampleFile(t, numSampleRows)
	wantB, err := io.ReadAll(f)
	require.NoError(t, err)

	rcr := &readCloseRecorder{Reader: f}
	src := streamcache.NewSource(rcr)

	r1, err := src.NewReadCloser()
	require.NoError(t, err)

	gotB1, err := ioutil.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, rcr.closed)

	r2, err := src.NewReadCloser()
	require.NoError(t, err)
	src.Seal()

	gotB2, err := ioutil.ReadAll(r2)
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
		//  1,A,B,C...
		//  2,A,B,C...
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
