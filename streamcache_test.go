package streamcache_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/neilotoole/streamcache"
)

const (
	numSampleRows = 4321
	numG          = 2000

	anything = "anything"
)

func TestStream(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	s := streamcache.New(strings.NewReader(anything))
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
	require.Equal(t, 0, s.Size())
	require.Nil(t, s.Err())
	requireNoTotal(t, s)

	r := s.NewReader(ctx)
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
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
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
	requireNoTotal(t, s)

	// Seal the source; after this, no more readers can be created.
	s.Seal()
	require.True(t, s.Sealed())
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
	requireNoTotal(t, s)

	require.Panics(t, func() {
		_ = s.NewReader(ctx)
	}, "should panic because cache is already sealed")

	// Read the remaining bytes.
	gotN, gotErr = r.Read(buf)
	require.NoError(t, gotErr)
	require.Nil(t, s.Err())
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
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
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())
	require.Equal(t, 8, streamcache.ReaderOffset(r))
	require.Equal(t, 8, s.Size())

	// Read one more time, and we should get io.EOF again.
	gotN, gotErr = r.Read(buf)
	require.Error(t, gotErr)
	require.Equal(t, 0, gotN)
	require.Equal(t, io.EOF, gotErr)
	require.Equal(t, io.EOF, s.Err())
	require.Equal(t, 8, streamcache.ReaderOffset(r))
	require.Equal(t, 8, s.Size())
	requireTotal(t, s, 8)
	requireNoTake(t, s.Done())
	requireTake(t, s.Filled())

	// Close the reader, which should close the underlying source.
	gotErr = r.Close()
	require.NoError(t, gotErr)
	requireTotal(t, s, 8)
	requireTake(t, s.Done())
	requireTake(t, s.Filled())

	// Closing again should be no-op.
	gotErr = r.Close()
	require.Nil(t, gotErr)
	requireTotal(t, s, 8)
	requireTake(t, s.Done())
	requireTake(t, s.Filled())
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
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
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
	requireNoTake(t, s.Done())
	require.NoError(t, r.Close())
	requireTake(t, s.Done())
}

func TestReader_NoSeal(t *testing.T) {
	t.Parallel()

	s := streamcache.New(strings.NewReader(anything))
	r := s.NewReader(context.Background())
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, anything, string(gotData))
	require.NoError(t, r.Close())
	requireNoTake(t, s.Done(), "not done because not sealed")
	requireTake(t, s.Filled())
	require.Equal(t, io.EOF, s.Err())
	requireTotal(t, s, len(anything))
}

func TestStream_File(t *testing.T) {
	ctx := context.Background()
	wantSize, fp := generateSampleFile(t, numSampleRows)
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
	requireTotal(t, s, wantSize)
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())
	require.Equal(t, wantSize, s.Size())
	require.True(t, errors.Is(s.Err(), io.EOF))

	require.Equal(t, string(wantData), string(gotData))

	assert.NoError(t, r.Close())
	assert.Equal(t, 1, recorder.closeCount)
	require.Equal(t, wantSize, s.Size())
	requireTake(t, s.Filled())
	requireTake(t, s.Done())
	requireTotal(t, s, wantSize)
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
			requireTake(t, s.Filled())
			requireTotal(t, s, wantSize)
		}(r)
	}

	requireNoTake(t, s.Done())

	s.Seal()

	<-s.Done()

	require.Equal(t, wantSize, s.Size())
	requireTotal(t, s, wantSize)
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
			require.NoError(t, gotErr)
			requireTotal(t, s, wantSize)
			requireTake(t, s.Filled())

			assert.Equal(t, string(wantData), string(gotData))
		}(i, rdrs[i])
	}

	t.Logf("Waiting on <-s.Done()")
	<-s.Done()

	assert.NoError(t, err)
	require.Equal(t, wantSize, s.Size())
	requireTotal(t, s, wantSize)
	requireTake(t, s.Filled())
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

	requireNoTotal(t, s)
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
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
	requireTotal(t, s, len(want))
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())

	r2 := s.NewReader(ctx)
	require.NoError(t, err)

	s.Seal()

	requireTotal(t, s, len(want))
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())

	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, want, string(gotData2))

	require.NotPanics(t, func() {
		s.Seal()
	}, "subsequent calls to s.Seal shouldn't panic")
}

func TestSeal_NoReaders(t *testing.T) {
	t.Parallel()

	s := streamcache.New(strings.NewReader(anything))
	s.Seal()
	requireTake(t, s.Done())
	requireNoTake(t, s.Filled())
	requireNoTotal(t, s)
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

	requireNoTake(t, s.Done())
	r1 := s.NewReader(ctx)

	gotData1, err := io.ReadAll(r1)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData1)
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())
	require.NoError(t, r1.Close())
	requireNoTake(t, s.Done())
	require.Equal(t, 0, recorder.closeCount)

	r2 := s.NewReader(ctx)
	s.Seal()

	requireNoTake(t, s.Done())
	gotData2, err := io.ReadAll(r2)
	require.NoError(t, err)
	require.Equal(t, wantData, gotData2)
	requireNoTake(t, s.Done())
	require.NoError(t, r2.Close())
	requireTake(t, s.Done())
	require.Equal(t, 1, recorder.closeCount)
}

// TestReader_Read_PartialCacheHit tests the scenario where
// a Reader.Read request is only partially satisfied by Stream's
// cache. Reader.Read will return the bytes available to
// it in the cache, thus the returned n may be < len(p).
func TestReader_Read_PartialCacheHit(t *testing.T) {
	ctx := context.Background()
	s := streamcache.New(strings.NewReader(anything))

	r1 := s.NewReader(ctx)
	buf1 := make([]byte, 3)
	n1, err := r1.Read(buf1)
	require.NoError(t, err)
	require.Equal(t, 3, n1)
	require.Equal(t, 3, s.Size())

	r2 := s.NewReader(ctx)
	buf2 := make([]byte, 5)
	n2, err := r2.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, 3, n2)
	require.Equal(t, 3, s.Size())

	buf2 = make([]byte, 10)
	n2, err = r2.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, 5, n2)
	require.Equal(t, len(anything), s.Size())

	r3 := s.NewReader(ctx)
	buf3 := make([]byte, 10)
	n3, err := io.ReadFull(r3, buf3)
	require.Equal(t, len(anything), n3)
	require.True(t, errors.Is(err, io.ErrUnexpectedEOF))
}

func TestEmptyStream_EOF(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := streamcache.New(strings.NewReader(""))

	r := s.NewReader(ctx)
	gotData, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "", string(gotData))
	requireNoTake(t, s.Done())
	requireTake(t, s.Filled())
	require.Equal(t, 0, s.Size())
	requireTotal(t, s, 0)
	require.Equal(t, io.EOF, s.Err())
}

func TestEmptyStream_NoEOF(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := &tweakableReader{}
	s := streamcache.New(src)

	r := s.NewReader(ctx)
	buf := make([]byte, 10)
	gotN, gotErr := r.Read(buf)
	require.NoError(t, gotErr)
	require.Equal(t, 0, gotN)
	requireNoTake(t, s.Done())
	requireNoTake(t, s.Filled())
	require.Equal(t, 0, s.Size())
	requireNoTotal(t, s)

	src.err = io.EOF
	gotN, gotErr = r.Read(buf)
	require.Equal(t, 0, gotN)
	require.True(t, errors.Is(gotErr, io.EOF))
	requireTake(t, s.Filled())
	requireNoTake(t, s.Done())
	require.Equal(t, 0, s.Size())
	requireTotal(t, s, 0)
}

// TestContextCancelBeforeSrcRead tests the scenario where
// Reader r2 is blocked due to r1 having the src lock,
// and then r2's context is canceled before the lock is released.
// On lock release, r2 proceeds and acquires the src lock, but
// instead of reading from src, r2 should return the cancellation
// cause before even attempting to read from the source.
func TestContextCancelBeforeSrcRead(t *testing.T) {
	t.Parallel()
	sleep := time.Millisecond * 100

	src := &tweakableReader{unblock: make(chan struct{}, 1), data: []byte(anything)}
	s := streamcache.New(src)

	r1 := s.NewReader(context.Background())
	// buf1 is zero length, because we don't actually
	// want to fill up the cache when r1 reads.
	buf1 := make([]byte, 0)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		// r1 will block until it receives from src.unblock.
		n, err := r1.Read(buf1)
		require.NoError(t, err)
		require.Equal(t, 0, n)
	}()

	time.Sleep(sleep)

	wantErr := errors.New("doh")
	ctx, cancelFn := context.WithCancelCause(context.Background())
	r2 := s.NewReader(ctx)
	buf2 := make([]byte, 10)
	go func() {
		defer wg.Done()
		// r2 will block on acquiring the src lock, until r1
		// releases it. But before r1 releases it, r2's context
		// will be cancelled, and thus wantErr should be returned.
		n, err := r2.Read(buf2)
		assert.Equal(t, 0, n)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, wantErr))
	}()

	time.Sleep(sleep)
	// r1 is blocked on src.unblock, and r2 is blocked on src lock.
	// Cancel r2's context.
	cancelFn(wantErr)
	time.Sleep(sleep)
	// Now, unblock r1, and r2 should then acquire the src lock,
	// but then r2 should consult its context, and return the
	// cancellation cause.
	src.unblock <- struct{}{}
	wg.Wait()
}

func TestStreamSource(t *testing.T) {
	t.Parallel()

	// Write some data to a test file.
	fp := filepath.Join(t.TempDir(), "streamcache_test.txt")
	require.NoError(t, os.WriteFile(fp, []byte(anything), 0o600))

	f, fErr := os.Open(fp)
	require.NoError(t, fErr)

	// Create a stream (and reader) that reads from the file.
	stream := streamcache.New(f)
	r := stream.NewReader(context.Background())

	// Read a small chunk from the file.
	buf := make([]byte, 2)
	n, readErr := r.Read(buf)
	require.NoError(t, readErr)
	require.Equal(t, 2, n)

	gotSrc := stream.Source()
	require.Equal(t, f, gotSrc)

	// Close the source (i.e. the file), and then try
	// to read from the reader.
	require.NoError(t, gotSrc.(io.ReadCloser).Close())

	n, readErr = r.Read(buf)
	require.Error(t, readErr)
	require.Equal(t, 0, n)
	readPathErr := new(os.PathError)
	require.True(t, errors.As(readErr, &readPathErr))
	require.Equal(t, "read", readPathErr.Op)
	require.Equal(t, "file already closed", readPathErr.Err.Error())
	require.Equal(t, 2, stream.Size())
	require.Equal(t, readErr, stream.Err())
	total, totalErr := stream.Total(context.Background())
	require.Error(t, totalErr)
	require.Equal(t, 2, total)
	require.True(t, errors.Is(totalErr, readErr))
	requireTake(t, stream.Filled())

	// Now check what happens when we close the reader.
	requireNoTake(t, stream.Done(),
		"stream is not done until sealed and reader is closed")
	stream.Seal()
	closeErr := r.Close()
	require.Error(t, closeErr)
	closePathErr := new(os.PathError)
	require.True(t, errors.As(closeErr, &closePathErr))
	require.Equal(t, "close", closePathErr.Op)
	require.Equal(t, "file already closed", closePathErr.Err.Error())
	requireTake(t, stream.Done())
}
