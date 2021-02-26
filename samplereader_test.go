package samplereader_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
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

func TestSimple(t *testing.T) {
	f := generateSampleFile(t, 100000)
	rc := &readCloser{Reader: f}
	src := samplereader.NewSource(rc)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	r, err := src.NewReader()
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

func TestSampleReaderConcurrent(t *testing.T) {
	f := generateSampleFile(t, 100000)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	rc := &readCloser{Reader: bytes.NewReader(wantB)}
	sr := samplereader.NewSource(rc)
	require.NoError(t, err)

	g := &errgroup.Group{}
	for i := 0; i < 10000; i++ {
		i := i
		r, err := sr.NewReader()
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

			if bytes.Equal(wantB, gotB) {
				t.Logf("goroutine %d: SUCCESS", i) // FIXME: remove
				return nil
			}
			assert.Equal(t, wantB, gotB)
			return nil
		})
	}
	sr.Seal()
	err = g.Wait()
	assert.NoError(t, err)
	assert.Equal(t, 1, rc.closed)
}

func TestSeal(t *testing.T) {
	sr := samplereader.NewSource(strings.NewReader(""))
	r, err := sr.NewReader()
	require.NoError(t, err)
	require.NotNil(t, r)

	sr.Seal()
	r, err = sr.NewReader()
	require.Error(t, err)
	require.Equal(t, samplereader.ErrSealed, err)
	require.Nil(t, r)
}

func TestClose(t *testing.T) {
	f := generateSampleFile(t, 1000)
	wantB, err := ioutil.ReadAll(f)
	require.NoError(t, err)

	rc := &readCloser{Reader: f}
	src := samplereader.NewSource(rc)

	r1, err := src.NewReader()
	require.NoError(t, err)

	gotB1, err := ioutil.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, rc.closed)

	r2, err := src.NewReader()
	require.NoError(t, err)
	src.Seal()

	gotB2, err := ioutil.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB2)
	require.NoError(t, r1.Close())
	require.Equal(t, 1, rc.closed)
}

func TestGenerateSample(t *testing.T) {
	generateSampleData(t)
}

func generateSampleData(t *testing.T) {
	f, err := os.Create(filepath.Join("testdata", "sample1.csv"))
	if err != nil {
		t.Fatal(err)
	}

	defer f.Close()

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z\n"
	for i := 0; i < 10000; i++ {
		_, err = f.WriteString(strconv.Itoa(i) + "," + line)
		if err != nil {
			t.Fatal(err)
		}
	}
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

type readCloser struct {
	io.Reader
	closed int
	mu     sync.Mutex
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
