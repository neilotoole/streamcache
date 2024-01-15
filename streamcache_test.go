package streamcache_test

import (
	"bytes"
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

	"github.com/neilotoole/streamcache"
)

const (
	//numSampleRows = 1000000
	numSampleRows = 4000
	numG          = 500
	jitterFactor  = 30
)

var _ io.ReadCloser = (*streamcache.Reader)(nil)

func TestRead_SingleReaderImmediateSeal(t *testing.T) {
	//t.Parallel()
	want := []byte("anything")
	src := streamcache.NewSource(bytes.NewReader(want))
	r, err := src.NewReader(context.Background())
	require.NoError(t, err)
	src.Seal()

	gotBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, string(want), string(gotBytes))
	require.NoError(t, r.Close())
}

func TestRead_NoSeal(t *testing.T) {
	//t.Parallel()
	want := []byte("anything")
	src := streamcache.NewSource(bytes.NewReader(want))
	r, err := src.NewReader(context.Background())
	require.NoError(t, err)

	gotBytes, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, string(want), string(gotBytes))
	require.NoError(t, r.Close())
}

func TestBasic(t *testing.T) {
	ctx := context.Background()
	_, fp := generateSampleFile(t, numSampleRows)

	fi, err := os.Stat(fp)
	require.NoError(t, err)
	t.Logf("Sample file size: %d", fi.Size())

	wantB, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)
	rcr := &rcRecorder{r: f}
	src := streamcache.NewSource(rcr)

	r, err := src.NewReader(ctx)
	require.NoError(t, err)

	src.Seal()

	defer func() {
		assert.NoError(t, r.Close())
		assert.Equal(t, 1, rcr.closeCount)
		require.Equal(t, fi.Size(), int64(src.Size()))
	}()

	gotB, err := io.ReadAll(r)
	require.NoError(t, err)

	require.Equal(t, string(wantB), string(gotB))
}

func TestConcurrent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, fp := generateSampleFile(t, numSampleRows)
	wantB, err := os.ReadFile(fp)
	require.NoError(t, err)
	fi, err := os.Stat(fp)
	require.NoError(t, err)
	t.Logf("Sample file size: %d", fi.Size())

	f, err := os.Open(fp)
	require.NoError(t, err)

	//rcr := &rcRecorder{Reader: bytes.NewReader(wantB)}
	src := streamcache.NewSource(f)
	require.NoError(t, err)

	wg := &sync.WaitGroup{}
	wg.Add(numG)

	go func() {}()

	//var rdrs []*streamcache.Reader

	//g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < numG; i++ {
		i := i
		r, err := src.NewReader(ctx)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			defer func() {
				println(fmt.Sprintf("r.Close for %d", i))
				assert.NoError(t, r.Close())
			}()

			// Add some jitter
			time.Sleep(time.Nanosecond * time.Duration(rand.Intn(jitterFactor)))

			_ = i
			x := i
			_ = x

			//if i > 10 {
			//	println("sealing her for real")
			//	src.Seal()
			//}

			println(fmt.Sprintf("goroutine %d: about to read all", i))
			gotB, err := io.ReadAll(r)
			assert.NoError(t, err)
			//if err != nil {
			//	return err
			//}

			//require.NoError(t, r.Close())

			assert.Equal(t, string(wantB), string(gotB))
		}()

	}

	wg.Wait()
	src.Seal()
	assert.NoError(t, err)
	//assert.Equal(t, 1, rcr.closed)

	require.Equal(t, fi.Size(), int64(src.Size()))
}

func sleepJitter() {
	d := time.Millisecond * time.Duration(rand.Intn(jitterFactor))
	//fmt.Printf("sleeping for %s\n", d)
	time.Sleep(d)
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
	src := streamcache.NewSource(recRdr)
	require.NoError(t, err)

	t.Logf("Iterations: %d", numG)

	rdrs := make([]*streamcache.Reader, numG)
	for i := 0; i < numG; i++ {
		rdrs[i], err = src.NewReader(ctx)
		require.NoError(t, err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(numG)
	sealOnce := &sync.Once{}

	for i := range rdrs {
		go func(i int, r *streamcache.Reader) {
			defer func() {
				//println(fmt.Sprintf("r.Close for %d", i))
				assert.NoError(t, r.Close())
				wg.Done()
			}()

			sleepJitter()

			if i > numG/2 {
				sealOnce.Do(func() {
					t.Logf("Sealing once on iter %d", i)
					src.Seal()
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

func TestSeal2(t *testing.T) {
	t.Parallel()

	want := strings.Repeat("anything", 100)

	ctx := context.Background()
	src := streamcache.NewSource(strings.NewReader(want))
	r1, err := src.NewReader(ctx)
	require.NoError(t, err)
	require.NotNil(t, r1)
	gotBytes1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, want, string(gotBytes1))

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)

	src.Seal()

	gotBytes2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, want, string(gotBytes2))

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
	src := streamcache.NewSource(recRdr)

	r1, err := src.NewReader(ctx)
	require.NoError(t, err)

	gotB1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, recRdr.closeCount)

	r2, err := src.NewReader(ctx)
	require.NoError(t, err)
	src.Seal()

	gotB2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantB, gotB2)
	require.NoError(t, r2.Close())
	require.Equal(t, 1, recRdr.closeCount)
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
