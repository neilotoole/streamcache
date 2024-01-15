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

var _ io.ReadCloser = (*Reader)(nil)

func isDone(src *Source) bool {
	select {
	case <-src.Done():
		return true
	default:
		return false
	}
}

func TestRead_MixedCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	src := NewSource(strings.NewReader(anything))

	require.False(t, isDone(src))
	select {
	case <-src.Done():
		t.Fatal("src.Done() should not be closed")
	default:
	}
	require.Equal(t, 0, src.Size())
	require.Nil(t, src.Err())

	r, err := src.NewReader(ctx)
	require.NoError(t, err)

	// We'll read half the bytes.
	buf := make([]byte, 4)
	gotN, gotErr := r.Read(buf)
	require.NoError(t, gotErr)
	require.Equal(t, 4, gotN)
	require.Equal(t, "anyt", string(buf))
	require.Equal(t, 4, r.offset)
	require.Equal(t, 4, src.size)
	require.Equal(t, 4, len(src.buf))
	require.Equal(t, 4, len(src.cache))

	// Seal the source; after this, no more readers can be created.
	require.NoError(t, src.Seal())
	require.True(t, src.Sealed())
	require.False(t, isDone(src))
	select {
	case <-src.Done():
		t.Fatal("src.Done() should not be closed")
	default:
	}

	// We shouldn't be able to create another reader, because
	// the source is sealed.
	r2, err := src.NewReader(ctx)
	require.Error(t, err)
	require.Equal(t, ErrAlreadySealed, err)
	require.Nil(t, r2)
	require.Nil(t, src.Err())

	// Read the remaining bytes.
	gotN, gotErr = r.Read(buf)
	require.NoError(t, gotErr)
	require.Nil(t, src.Err())
	require.Equal(t, 4, gotN)
	require.Equal(t, "hing", string(buf))
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, src.size)

	// Read again, but this time we should get io.EOF.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, src.Err())
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, src.Size())
	require.False(t, isDone(src))

	// Read one more time, and we should get io.EOF again.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, src.Err())
	require.Equal(t, 8, r.offset)
	require.Equal(t, 8, src.Size())
	require.False(t, isDone(src))

	// Close the reader, which should close the underlying source.
	gotErr = r.Close()
	require.NoError(t, gotErr)
	require.True(t, isDone(src))

	select {
	case <-src.Done():
		// Expected
	default:
		t.Fatal("src.Done() should be closed")
	}

	// Closing again should be no-op.
	gotErr = r.Close()
	require.Nil(t, gotErr)
	require.True(t, isDone(src))
}

func TestReaderAlreadyClosed(t *testing.T) {
	src := NewSource(strings.NewReader(anything))
	r, err := src.NewReader(context.Background())
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

func TestRead_SingleReaderImmediateSeal(t *testing.T) {
	src := NewSource(strings.NewReader(anything))
	r, err := src.NewReader(context.Background())
	require.NoError(t, err)

	require.NoError(t, src.Seal())

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	require.True(t, isDone(src))
}

func TestRead_NoSeal(t *testing.T) {
	// t.Parallel()
	src := NewSource(strings.NewReader(anything))
	r, err := src.NewReader(context.Background())
	require.NoError(t, err)

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	require.False(t, isDone(src), "not closed because not sealed")
	require.Equal(t, io.EOF, src.Err())
}

func TestRead_File(t *testing.T) {
	ctx := context.Background()
	_, fp := generateSampleFile(t, numSampleRows)

	fi, err := os.Stat(fp)
	require.NoError(t, err)
	t.Logf("Sample file size: %d", fi.Size())

	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)
	rcr := &rcRecorder{r: f}
	src := NewSource(rcr)

	r, err := src.NewReader(ctx)
	require.NoError(t, err)

	require.NoError(t, src.Seal())

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, string(wantData), string(gotData))

	assert.NoError(t, r.Close())
	assert.Equal(t, 1, rcr.closeCount)
	require.Equal(t, fi.Size(), int64(src.Size()))
}

func TestPointer(t *testing.T) {
	// FIXME: delete this test
	var pCloseErr *error
	require.Nil(t, pCloseErr)

	var closeErr error
	require.Nil(t, closeErr)

	pCloseErr = &closeErr
	require.NotNil(t, pCloseErr)

	gotCloseErr := *pCloseErr
	require.Nil(t, gotCloseErr)

	actualErr := errors.New("oh noes")
	pCloseErr = &actualErr
	require.NotNil(t, pCloseErr)
	require.NotNil(t, *pCloseErr)
}

func TestRead_File_Concurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)
	f, err := os.Open(fp)
	require.NoError(t, err)

	src := NewSource(f)
	for i := 0; i < numG; i++ {
		r, err := src.NewReader(ctx)
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
	case <-src.Done():
		t.Fatal("Shouldn't be done because not sealed")
	default:
	}

	require.NoError(t, src.Seal())

	<-src.Done()

	require.Equal(t, wantSize, src.Size())
}

func TestConcurrent2(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)

	recRdr := &rcRecorder{r: f}
	src := NewSource(recRdr)
	require.NoError(t, err)

	t.Logf("Iterations: %d", numG)

	rdrs := make([]*Reader, numG)
	for i := 0; i < numG; i++ {
		rdrs[i], err = src.NewReader(ctx)
		require.NoError(t, err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(numG)
	sealOnce := &sync.Once{}

	for i := range rdrs {
		go func(i int, r *Reader) {
			defer func() {
				// println(fmt.Sprintf("r.Close for %d", i))
				assert.NoError(t, r.Close())
				wg.Done()
			}()

			sleepJitter()

			if i > numG/2 {
				sealOnce.Do(func() {
					t.Logf("Sealing once on iter %d", i)
					require.NoError(t, src.Seal())
				})
			}

			gotData, gotErr := io.ReadAll(r)
			assert.NoError(t, gotErr)

			assert.Equal(t, string(wantData), string(gotData))
		}(i, rdrs[i])
	}

	sleepJitter()
	wg.Wait()

	assert.NoError(t, err)
	require.Equal(t, wantSize, src.Size())
}

func TestSeal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := NewSource(strings.NewReader("anything"))
	r, err := src.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r)

	require.NoError(t, src.Seal())

	r, err = src.NewReader(ctx)
	require.Error(t, err)
	require.Equal(t, ErrAlreadySealed, err)
	require.Nil(t, r)
}

func TestSeal2(t *testing.T) {
	t.Parallel()

	want := strings.Repeat("anything", 100)

	ctx := context.Background()
	src := NewSource(strings.NewReader(want))
	r1, err := src.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r1)
	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData1))

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)

	require.NoError(t, src.Seal())

	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData2))
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, fp := generateSampleFile(t, numSampleRows)
	wantB, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)
	recRdr := &rcRecorder{r: f}
	src := NewSource(recRdr)

	r1, err := src.NewReader(ctx)
	require.NoError(t, err)

	gotB1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, recRdr.closeCount)

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)
	require.NoError(t, src.Seal())

	gotB2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB2)
	require.NoError(t, r2.Close())
	require.Equal(t, 1, recRdr.closeCount)
}

// generateSampleFile generates a temp file of sample data with the
// specified number of rows. It is the caller's responsibility to
// close the file. Note that the file is removed by t.Cleanup.
func generateSampleFile(t *testing.T, rows int) (size int, fp string) { //nolint:unparam
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

// rcRecorder wraps an  and records stats.
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

func TestContextAwareness(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("oh noes")
	originRdr := DelayReader(LimitRandReader(100000), time.Second, true)
	src := NewSource(originRdr)

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
	src := NewSource(originRdr)

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

func sleepJitter() {
	d := time.Millisecond * time.Duration(rand.Intn(jitterFactor))
	time.Sleep(d)
}
