package samplereader_test

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

	"github.com/neilotoole/samplereader"
)

const sampleRows = 100000

var _ io.ReadCloser = (*samplereader.ReadCloser)(nil)

func TestSimple(t *testing.T) {
	f := generateSampleFile(t, sampleRows)
	rc := &readCloser{Reader: f}
	src := samplereader.NewSource(rc)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	r, err := src.NewReadCloser()
	require.NoError(t, err)
	src.Seal()

	defer func() {
		assert.NoError(t, r.Close())
		assert.Equal(t, 1, rc.closed)
	}()

	gotB, err := ioutil.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, wantB, gotB)
}

func TestConcurrent(t *testing.T) {
	f := generateSampleFile(t, sampleRows)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	rc := &readCloser{Reader: bytes.NewReader(wantB)}
	src := samplereader.NewSource(rc)
	require.NoError(t, err)

	g := &errgroup.Group{}
	for i := 0; i < 1000; i++ {
		r, err := src.NewReadCloser()
		require.NoError(t, err)

		g.Go(func() error {
			defer func() {
				assert.NoError(t, r.Close())
			}()

			// Add some jitter
			time.Sleep(time.Nanosecond * time.Duration(rand.Intn(5000)))

			gotB, err := ioutil.ReadAll(r)
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
	assert.Equal(t, 1, rc.closed)
}

func TestSeal(t *testing.T) {
	src := samplereader.NewSource(strings.NewReader(""))
	r, err := src.NewReadCloser()
	require.NoError(t, err)
	require.NotNil(t, r)

	src.Seal()
	r, err = src.NewReadCloser()
	require.Error(t, err)
	require.Equal(t, samplereader.ErrSealed, err)
	require.Nil(t, r)
}

func TestClose(t *testing.T) {
	f := generateSampleFile(t, sampleRows)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	rc := &readCloser{Reader: f}
	src := samplereader.NewSource(rc)

	r1, err := src.NewReadCloser()
	require.NoError(t, err)

	gotB1, err := ioutil.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, rc.closed)

	r2, err := src.NewReadCloser()
	require.NoError(t, err)
	src.Seal()

	gotB2, err := ioutil.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB2)
	require.NoError(t, r2.Close())
	require.Equal(t, 1, rc.closed)
}

// generateSampleFile generates a temp file of sample data with the
// specified number of rows. It is the caller's responsibility to
// close the file. Note that the file is removed by t.Cleanup.
func generateSampleFile(t *testing.T, rows int) *os.File {
	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(f.Name()))
	})

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z\n"
	for i := 0; i < rows; i++ {
		_, err = f.WriteString(strconv.Itoa(i) + "," + line)
		require.NoError(t, err)
	}

	return f
}

// readCloser is used to verify that Close was invoked the
// expected number of times.
type readCloser struct {
	mu sync.Mutex
	io.Reader
	closed int
}

func (rc *readCloser) Close() error {
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
