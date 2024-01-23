package streamcache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neilotoole/streamcache"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	numSampleRows = 4321
	numG          = 500

	anything = "anything"
)

func TestStream(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s := streamcache.New(strings.NewReader(anything))
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	require.Equal(t, 0, s.Size())
	require.Nil(t, s.Err())
	require.Equal(t, -1, s.ErrAt())
	requireNoTotal(t, s)

	r := s.NewReader(ctx)
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	requireNoTotal(t, s)

	// We'll read half the bytes.
	buf := make([]byte, 4)
	gotN, gotErr := r.Read(buf)
	require.NoError(t, gotErr)
	require.Equal(t, 4, gotN)
	require.Equal(t, "anyt", string(buf))
	require.Equal(t, 4, streamcache.ReaderOffset(r))
	require.Equal(t, 4, s.Size())
	require.Equal(t, 4, len(streamcache.CacheInternal(s)))
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	requireNoTotal(t, s)

	// Seal the source; after this, no more readers can be created.
	s.Seal()
	require.True(t, s.Sealed())
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	requireNoTotal(t, s)

	require.Panics(t, func() {
		_ = s.NewReader(ctx)
	}, "should panic because cache is already sealed")

	// Read the remaining bytes.
	gotN, gotErr = r.Read(buf)
	require.NoError(t, gotErr)
	require.Nil(t, s.Err())
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	requireNoTotal(t, s)
	require.Equal(t, 4, gotN)
	require.Equal(t, "hing", string(buf))
	require.Equal(t, 8, streamcache.ReaderOffset(r))
	require.Equal(t, 8, s.Size())

	// Read again, but this time we should get io.EOF.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, s.Err())
	requireTotal(t, s, 8)
	requireTake(t, s.SourceDone())
	requireNoTake(t, s.ReadersDone())
	require.Equal(t, 8, streamcache.ReaderOffset(r))
	require.Equal(t, 8, s.Size())
	require.Equal(t, 8, s.ErrAt())

	// Read one more time, and we should get io.EOF again.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, s.Err())
	require.Equal(t, 8, streamcache.ReaderOffset(r))
	require.Equal(t, 8, s.Size())
	require.Equal(t, 8, s.ErrAt())
	requireTotal(t, s, 8)
	requireNoTake(t, s.ReadersDone())
	requireTake(t, s.SourceDone())

	// Close the reader, which should close the underlying source.
	gotErr = r.Close()
	require.NoError(t, gotErr)
	requireTotal(t, s, 8)
	requireTake(t, s.ReadersDone())
	requireTake(t, s.SourceDone())

	// Closing again should be no-op.
	gotErr = r.Close()
	require.Nil(t, gotErr)
	requireTotal(t, s, 8)
	requireTake(t, s.ReadersDone())
	requireTake(t, s.SourceDone())
}

func TestReaderAlreadyClosed(t *testing.T) {
	s := streamcache.New(strings.NewReader(anything))
	r := s.NewReader(context.Background())
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	require.NoError(t, err)

	// After closing, we should get ErrAlreadyClosed if we try to read again.
	require.NoError(t, r.Close())
	_, err = r.Read(buf)
	require.Error(t, err)
	require.Equal(t, streamcache.ErrAlreadyClosed, err)
	requireNoTotal(t, s)
	requireNoTake(t, s.ReadersDone())
	requireNoTake(t, s.SourceDone())
	requireNoTotal(t, s)
}

func TestSingleReaderImmediateSeal(t *testing.T) {
	t.Parallel()

	s := streamcache.New(strings.NewReader(anything))
	r := s.NewReader(context.Background())
	s.Seal()

	requireNoTotal(t, s)
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	requireTotal(t, s, len(anything))
	require.Equal(t, anything, string(gotData))
	requireNoTake(t, s.ReadersDone())
	require.NoError(t, r.Close())
	requireTake(t, s.ReadersDone())
}

func TestReader_NoSeal(t *testing.T) {
	t.Parallel()

	s := streamcache.New(strings.NewReader(anything))
	r := s.NewReader(context.Background())
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	requireNoTake(t, s.ReadersDone(), "not done because not sealed")
	requireTake(t, s.SourceDone())
	require.Equal(t, io.EOF, s.Err())
	requireTotal(t, s, len(anything))
}

func TestStream_File(t *testing.T) {
	ctx := context.Background()
	sampleSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)
	recorder := &rcRecorder{r: f}
	s := streamcache.New(recorder)

	r := s.NewReader(ctx)
	require.NoError(t, err)

	s.Seal()

	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	requireTotal(t, s, sampleSize)
	requireTake(t, s.SourceDone())
	requireNoTake(t, s.ReadersDone())
	require.Equal(t, sampleSize, s.Size())
	require.True(t, errors.Is(s.Err(), io.EOF))

	require.Equal(t, string(wantData), string(gotData))

	assert.NoError(t, r.Close())
	assert.Equal(t, 1, recorder.closeCount)
	require.Equal(t, sampleSize, s.Size())
	requireTake(t, s.SourceDone())
	requireTake(t, s.ReadersDone())
	requireTotal(t, s, sampleSize)
}

func TestStream_File_Concurrent_SealLate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)
	f, err := os.Open(fp)
	require.NoError(t, err)

	s := streamcache.New(f)
	for i := 0; i < numG; i++ {
		r := s.NewReader(ctx)
		require.NoError(t, err)

		go func(r *streamcache.Reader) {
			defer func() { assert.NoError(t, r.Close()) }()

			sleepJitter()

			gotData, err := io.ReadAll(r)
			assert.NoError(t, err)
			assert.Equal(t, string(wantData), string(gotData))
		}(r)
	}

	requireNoTake(t, s.ReadersDone())

	s.Seal()

	<-s.ReadersDone()

	require.Equal(t, wantSize, s.Size())
}

func TestStream_File_Concurrent_SealMiddle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
	wantData, err := os.ReadFile(fp)
	require.NoError(t, err)

	f, err := os.Open(fp)
	require.NoError(t, err)

	recorder := &rcRecorder{r: f}
	s := streamcache.New(recorder)
	require.NoError(t, err)

	t.Logf("Iterations: %d", numG)

	rdrs := make([]*streamcache.Reader, numG)
	for i := 0; i < numG; i++ {
		rdrs[i] = s.NewReader(ctx)
		rdrs[i].Name = fmt.Sprintf("rdr-%d", i)
	}

	// This time, we'll seal in the middle of the reads.
	sealOnce := &sync.Once{}

	for i := range rdrs {
		go func(i int, r *streamcache.Reader) {
			defer func() {
				assert.NoError(t, r.Close())
			}()

			sleepJitter()

			if i > numG/2 {
				sealOnce.Do(func() {
					t.Logf("Sealing once on iter %d", i)
					s.Seal()
					t.Logf("SEALED once on iter %d", i)
				})
			}

			gotData, gotErr := io.ReadAll(r)
			assert.NoError(t, gotErr)

			assert.Equal(t, string(wantData), string(gotData))
		}(i, rdrs[i])
	}

	t.Logf("Waiting on <-s.ReadersDone()")
	<-s.ReadersDone()

	assert.NoError(t, err)
	require.Equal(t, wantSize, s.Size())
}

func TestSeal_AlreadySealed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := streamcache.New(strings.NewReader(anything))
	_ = s.NewReader(ctx)

	s.Seal()

	require.Panics(t, func() {
		_ = s.NewReader(ctx)
	}, "should panic because stream is already sealed")
}

func TestSeal_AfterRead(t *testing.T) {
	t.Parallel()

	want := strings.Repeat(anything, 100)
	ctx := context.Background()
	s := streamcache.New(strings.NewReader(want))
	r1 := s.NewReader(ctx)
	require.NotNil(t, r1)
	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData1))

	r2 := s.NewReader(ctx)
	require.NoError(t, err)

	s.Seal()

	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData2))
}

func TestContextAwareness(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("oh noes")
	srcRdr := newDelayReader(newLimitRandReader(100000), time.Second, true)
	s := streamcache.New(srcRdr)

	ctx := context.Background()
	ctx, cancel := context.WithCancelCause(ctx)
	time.AfterFunc(time.Second, func() {
		cancel(wantErr)
	})

	r := s.NewReader(ctx)
	_, gotErr := io.ReadAll(r)
	require.Error(t, gotErr)
	require.True(t, errors.Is(gotErr, wantErr))
}

func TestErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantErr := errors.New("oh noes")
	const errAfterN = 50

	s := streamcache.New(newErrorAfterNReader(errAfterN, wantErr))

	r1 := s.NewReader(ctx)
	gotData1, err := io.ReadAll(r1)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(gotData1))

	r2 := s.NewReader(ctx)
	gotData2, err := io.ReadAll(r2)
	require.Error(t, err)
	require.True(t, errors.Is(err, wantErr))
	require.Equal(t, errAfterN, len(gotData2))
}

func TestSizeTotal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	wantData := make([]byte, 0)
	s := streamcache.New(bytes.NewReader(wantData))
	require.Equal(t, 0, s.Size())
	requireNoTotal(t, s)

	r := s.NewReader(ctx)
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData)
	require.Equal(t, 0, s.Size())
	requireTotal(t, s, 0)
}

func TestClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	wantData := []byte(anything)
	recorder := &rcRecorder{r: strings.NewReader(anything)}
	s := streamcache.New(recorder)

	r1 := s.NewReader(ctx)

	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData1)
	require.NoError(t, r1.Close())
	require.Equal(t, 0, recorder.closeCount)

	r2 := s.NewReader(ctx)
	s.Seal()

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
