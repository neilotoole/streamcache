package streamcache_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/neilotoole/streamcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkDevelop is used during development as a standard benchmark
// for A/B testing.
//
//	$ go test -count=10 -bench BenchmarkDevelop > old.bench.txt
//	$ go test -count=10 -bench BenchmarkDevelop > new.bench.txt
//	$ benchstat old.bench.txt new.bench.txt
//
// The above is a useful way to compare the performance of different
// implementations.
func BenchmarkDevelop(b *testing.B) {
	const testBlobContents = false
	const numReaders = 1389

	initBlobs()
	wantBlob := blobs[size100KB]
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		stream := streamcache.New(bytes.NewReader(wantBlob))
		rdrs := make([]*streamcache.Reader, numReaders)
		for j := range rdrs {
			rdrs[j] = stream.NewReader(ctx)
		}

		stream.Seal()

		wg := &sync.WaitGroup{}
		wg.Add(len(rdrs))
		for j := range rdrs {
			go func(j int) {
				defer wg.Done()
				defer rdrs[j].Close()
				p, err := io.ReadAll(rdrs[j])
				assert.NoError(b, err)
				assert.Equal(b, len(wantBlob), len(p))
				if testBlobContents {
					assert.Equal(b, wantBlob, p)
				}
			}(j)
		}
		wg.Wait()
		requireTake(b, stream.Done())
	}

	b.StopTimer()
}

func BenchmarkReaders_RunParallel(b *testing.B) {
	// I'm not sure if this benchmark setup is correct.
	// See also BenchmarkReaders_Goroutines.
	initBlobs()

	for _, blobSize := range blobSizes {
		wantBlob := blobs[blobSize]

		b.Run("blob-"+humanSize(blobSize), func(b *testing.B) {
			for _, sealed := range []bool{true, false} {
				sealed := sealed
				b.Run(cond(sealed, "sealed", "unsealed"), func(b *testing.B) {
					stream := streamcache.New(bytes.NewReader(wantBlob))
					rdrCount := &atomic.Int64{}
					b.ReportAllocs()
					b.ResetTimer()

					b.RunParallel(func(pb *testing.PB) {
						ctx := context.Background()
						for pb.Next() {
							rdr := stream.NewReader(ctx)
							if sealed && rdrCount.Add(1) == int64(b.N) {
								stream.Seal()
							}

							p, err := io.ReadAll(rdr)
							assert.NoError(b, err)
							assert.Equal(b, len(wantBlob), len(p))
							assert.NoError(b, rdr.Close())
						}
					})
				})
			}
		})
	}
}

func BenchmarkReaders_Goroutines(b *testing.B) {
	initBlobs()

	for _, blobSize := range blobSizes {
		wantBlob := blobs[blobSize]

		b.Run("blob-"+humanSize(blobSize), func(b *testing.B) {
			for _, numReaders := range rdrCounts {
				numReaders := numReaders

				b.Run("rdrs-"+strconv.Itoa(numReaders), func(b *testing.B) {
					for _, sealed := range []bool{true, false} {
						sealed := sealed
						b.Run(cond(sealed, "sealed", "unsealed"), func(b *testing.B) {
							b.ReportAllocs()
							b.ResetTimer()

							ctx := context.Background()

							for i := 0; i < b.N; i++ {
								stream := streamcache.New(bytes.NewReader(wantBlob))
								rdrs := make([]*streamcache.Reader, numReaders)
								for j := range rdrs {
									rdrs[j] = stream.NewReader(ctx)
								}

								if sealed {
									stream.Seal()
								}

								wg := &sync.WaitGroup{}
								wg.Add(len(rdrs))
								for j := range rdrs {
									go func(j int) {
										defer wg.Done()
										defer rdrs[j].Close()
										p, err := io.ReadAll(rdrs[j])
										assert.NoError(b, err)
										assert.Equal(b, len(wantBlob), len(p))
									}(j)
								}
								wg.Wait()
							}
						})
					}
				})
			}
		})
	}
}

// BenchmarkVsStdlib compares streamcache, using a single Reader,
// to stdlib's bytes.Reader.
func BenchmarkVsStdlib(b *testing.B) {
	initBlobs()

	for _, blobSize := range blobSizes {
		wantBlob := blobs[blobSize]

		b.Run("blob-"+humanSize(blobSize), func(b *testing.B) {
			b.Run("stdlib-bytes-reader", func(b *testing.B) {
				newRdrFn := func() io.ReadCloser {
					return io.NopCloser(bytes.NewReader(wantBlob))
				}

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					readAll(b, newRdrFn, len(wantBlob))
				}
			})

			b.Run("streamcache-sealed", func(b *testing.B) {
				newRdrFn := func() io.ReadCloser {
					stream := streamcache.New(io.NopCloser(bytes.NewReader(wantBlob)))
					r := stream.NewReader(context.Background())
					stream.Seal()
					return r
				}

				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					readAll(b, newRdrFn, len(wantBlob))
				}
			})

			b.Run("streamcache-unsealed", func(b *testing.B) {
				newRdrFn := func() io.ReadCloser {
					stream := streamcache.New(io.NopCloser(bytes.NewReader(wantBlob)))
					r := stream.NewReader(context.Background())
					return r
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					readAll(b, newRdrFn, len(wantBlob))
				}
			})
		})
	}
}

func readAll(b *testing.B, newRdrFn func() io.ReadCloser, wantSize int) {
	b.Helper()
	rc := newRdrFn()
	p, err := io.ReadAll(rc)
	require.NoError(b, err)
	require.Equal(b, wantSize, len(p))
	require.NoError(b, rc.Close())
}

// humanSize produces a human-friendly byte size, e.g. 1.2MB.
func humanSize(i int) string {
	sizes := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	base := float64(1000)
	if i < 10 {
		return fmt.Sprintf("%dB", i)
	}
	e := math.Floor(math.Log(float64(i)) / math.Log(base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(i)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}

	return fmt.Sprintf(f, val, suffix)
}

// cond returns a if cond is true, else b.
func cond[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

// gCounts is the number of readers to use in various tests.
var rdrCounts = []int{
	1,
	2,
	// 3,
	// 4,
	7,
	// 16,
	93,
	// 531 ,
	17001,
	// 93125,
}

const (
	size0B    = 0
	size1B    = 1
	size9B    = 9
	size1K    = 1537
	size12KB  = 12289
	size100KB = 98304
	size786KB = 786432
	size6MB   = 6291456
	size50MB  = 50331649
	size500MB = 502653184
)

var blobSizes = []int{
	size0B,
	size1B,
	size9B,
	size1K,
	size12KB,
	size100KB,
	size786KB,
	size6MB,
	size50MB,
	size500MB,
}

// blobs is a map of blob sizes to the blob data. The blob data
// is random bytes of the given size, generated once via initBlobs.
var (
	blobs     = make(map[int][]byte, len(blobSizes))
	blobsOnce sync.Once
)

func initBlobs() {
	blobsOnce.Do(func() {
		var err error
		for _, size := range blobSizes {
			if blobs[size], err = io.ReadAll(newLimitRandReader(int64(size))); err != nil {
				panic(err)
			}
		}
	})
}
