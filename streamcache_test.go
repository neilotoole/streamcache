package streamcache

import (
	"context"
	"errors"
	"fmt"
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
)

const (
	numSampleRows = 40000
	numG          = 500
	jitterFactor  = 30
	anything      = "anything"
)

func TestCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cache := New(strings.NewReader(anything))
	require.False(t, isDone(cache))
	select {
	case <-cache.Done():
		t.Fatal("src.Done() should not be closed")
	default:
	}
	require.Equal(t, 0, cache.Size())
	require.Nil(t, cache.Err())

	r, err := cache.NewReader(ctx)
	require.NoError(t, err)

	// We'll read half the bytes.
	buf := make([]byte, 4)
	gotN, gotErr := r.Read(buf)
	require.NoError(t, gotErr)
	require.Equal(t, 4, gotN)
	require.Equal(t, "anyt", string(buf))
	require.Equal(t, 4, r.offset)
	require.Equal(t, 4, cache.size)
	require.Equal(t, 4, len(cache.buf))
	require.Equal(t, 4, len(cache.cache))

	// Seal the source; after this, no more readers can be created.
	require.NoError(t, cache.Seal())
	require.True(t, cache.Sealed())
	require.False(t, isDone(cache))
	select {
	case <-cache.Done():
		t.Fatal("src.Done() should not be closed")
	default:
	}

	// We shouldn't be able to create another reader, because
	// the source is sealed.
	r2, err := cache.NewReader(ctx)
	require.Error(t, err)
	require.Equal(t, ErrAlreadySealed, err)
	require.Nil(t, r2)
	require.Nil(t, cache.Err())

	// Read the remaining bytes.
	gotN, gotErr = r.Read(buf)
	require.NoError(t, gotErr)
	require.Nil(t, cache.Err())
	require.Equal(t, 4, gotN)
	require.Equal(t, "hing", string(buf))
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, cache.size)

	// Read again, but this time we should get io.EOF.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, cache.Err())
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, cache.Size())
	require.False(t, isDone(cache))

	// Read one more time, and we should get io.EOF again.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, cache.Err())
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, cache.Size())
	require.False(t, isDone(cache))

	// Close the reader, which should close the underlying source.
	gotErr = r.Close()
	require.NoError(t, gotErr)
	require.True(t, isDone(cache))

	select {
	case <-cache.Done():
		// Expected
	default:
		t.Fatal("cache.Done() should be closed")
	}

	// Closing again should be no-op.
	gotErr = r.Close()
	require.Nil(t, gotErr)
	require.True(t, isDone(cache))
}

func TestReaderAlreadyClosed(t *testing.T) {
	cache := New(strings.NewReader(anything))
	r, err := cache.NewReader(context.Background())
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = r.Read(buf)
	require.NoError(t, err)

	// After closing, we should get ErrAlreadyClosed if we try to read again.
	require.NoError(t, r.Close())
	_, err = r.Read(buf)
	require.Error(t, err)
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestSingleReaderImmediateSeal(t *testing.T) {
	t.Parallel()

	cache := New(strings.NewReader(anything))
	r, err := cache.NewReader(context.Background())
	require.NoError(t, err)

	require.NoError(t, cache.Seal())

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	require.True(t, isDone(cache))
}

func TestReader_NoSeal(t *testing.T) {
	t.Parallel()

	cache := New(strings.NewReader(anything))
	r, err := cache.NewReader(context.Background())
	require.NoError(t, err)

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	require.False(t, isDone(cache), "not closed because not sealed")
	require.Equal(t, io.EOF, cache.Err())
}

func TestCache_File(t *testing.T) {
	ctx := context.Background()
	_, fp := generateSampleFile(t, numSampleRows)

	fi, err := os.Stat(fp)
	require.NoError(t, err)
	t.Logf("Sample file size: %d", fi.Size())

	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)
	recorder := &rcRecorder{r: f}
	cache := New(recorder)

	r, err := cache.NewReader(ctx)
	require.NoError(t, err)

	require.NoError(t, cache.Seal())

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, string(wantData), string(gotData))

	assert.NoError(t, r.Close())
	assert.Equal(t, 1, recorder.closeCount)
	require.Equal(t, fi.Size(), int64(cache.Size()))
}

func TestCache_File_Concurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)
	f, err := os.Open(fp)
	require.NoError(t, err)

	cache := New(f)
	for i := 0; i < numG; i++ {
		r, err := cache.NewReader(ctx)
		require.NoError(t, err)

		go func(r *Reader) {
			defer func() { assert.NoError(t, r.Close()) }()

			sleepJitter()

			gotData, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Equal(t, string(wantData), string(gotData))
		}(r)
	}

	select {
	case <-cache.Done():
		t.Fatal("Shouldn't be done because not sealed")
	default:
	}

	require.NoError(t, cache.Seal())

	<-cache.Done()

	require.Equal(t, wantSize, cache.Size())
}

func TestCache_File_Concurrent2(t *testing.T) {
	t.Parallel()

	// FIXME: this test should be updated?

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)

	recorder := &rcRecorder{r: f}
	cache := New(recorder)
	require.NoError(t, err)

	t.Logf("Iterations: %d", numG)

	rdrs := make([]*Reader, numG)
	for i := 0; i < numG; i++ {
		rdrs[i], err = cache.NewReader(ctx)
		require.NoError(t, err)
	}

	// This time, we'll seal in the middle of the reads.
	sealOnce := &sync.Once{}

	for i := range rdrs {
		go func(i int, r *Reader) {
			defer func() {
				assert.NoError(t, r.Close())
			}()

			sleepJitter()

			if i > numG/2 {
				sealOnce.Do(func() {
					t.Logf("Sealing once on iter %d", i)
					require.NoError(t, cache.Seal())
				})
			}

			gotData, gotErr := io.ReadAll(r)
			assert.NoError(t, gotErr)

			assert.Equal(t, string(wantData), string(gotData))
		}(i, rdrs[i])
	}

	<-cache.Done()

	assert.NoError(t, err)
	require.Equal(t, wantSize, cache.Size())
}

func TestSeal_AlreadSealed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cache := New(strings.NewReader(anything))
	r, err := cache.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r)

	require.NoError(t, cache.Seal())

	r, err = cache.NewReader(ctx)
	require.Error(t, err)
	require.Equal(t, ErrAlreadySealed, err)
	require.Nil(t, r)
}

func TestSeal_AfterRead(t *testing.T) {
	t.Parallel()

	want := strings.Repeat(anything, 100)

	ctx := context.Background()
	cache := New(strings.NewReader(want))
	r1, err := cache.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r1)
	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData1))

	r2, err := cache.NewReader(ctx)
	require.NoError(t, err)

	require.NoError(t, cache.Seal())

	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData2))
}

func TestContextAwareness(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("oh noes")
	originRdr := newDelayReader(newLimitRandReader(100000), time.Second, true)
	cache := New(originRdr)

	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)
	time.AfterFunc(time.Second, func() {
		cancel(wantErr)
	})

	r, err := cache.NewReader(ctx)
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

	cache := New(newErrorAfterNReader(errAfterN, wantErr))

	r1, err := cache.NewReader(ctx)
	require.NoError(t, err)
	gotData1, err := io.ReadAll(r1)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(gotData1))

	r2, err := cache.NewReader(ctx)
	require.NoError(t, err)
	gotData2, err := io.ReadAll(r2)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(gotData2))
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	wantData := []byte(anything)
	recorder := &rcRecorder{r: strings.NewReader(anything)}
	cache := New(recorder)

	r1, err := cache.NewReader(ctx)
	require.NoError(t, err)

	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, recorder.closeCount)

	r2, err := cache.NewReader(ctx)
	require.NoError(t, err)
	require.NoError(t, cache.Seal())

	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData2)
	require.NoError(t, r2.Close())
	require.Equal(t, 1, recorder.closeCount)
}

// generateSampleFile generates a temp file of sample data with the
// specified number of rows. It is the caller's responsibility to
// close the file. Note that the file is removed by t.Cleanup.
func generateSampleFile(t *testing.T, rows int) (size int, fp string) {
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)
	fp = f.Name()

	const line = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z"
	for i := 0; i < rows; i++ {
		// Actual data lines will look like:
		//  0,A,B,C...
		//  1,A,B,C...
		s := strconv.Itoa(i) + "," + line
		_, err = fmt.Fprintln(f, s)
		require.NoError(t, err)
	}

	require.NoError(t, f.Close())
	fi, err := os.Stat(fp)
	require.NoError(t, err)
	size = int(fi.Size())
	t.Logf("Generated sample file [%d]: %s", size, fp)
	return int(fi.Size()), fp
}

var _ io.Reader = (*rcRecorder)(nil)

// rcRecorder wraps an io.Reader and records stats.
type rcRecorder struct {
	mu         sync.Mutex
	r          io.Reader
	closeCount int
	size       int
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
	d := time.Millisecond * time.Duration(rand.Intn(jitterFactor))
	time.Sleep(d)
}

func isDone(cache *Cache) bool {
	select {
	case <-cache.Done():
		return true
	default:
		return false
	}
}
