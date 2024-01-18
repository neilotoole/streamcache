# fifomu: fifo-queue mutex

Package `fifomu` provides a `Mutex` whose `Lock` method returns
the lock to callers in FIFO order. This is unlike `sync.Mutex`, where a
single goroutine can repeatedly lock and unlock and relock the mutex
without handing off to other lock waiter goroutines (until after a 1ms
starvation threshold, at which point `sync.Mutex` enters "starvation mode"
for those starved waiters).

`fifomu.Mutex` implements the same methodset as `sync.Mutex` and thus is
a drop-in replacement. The code is copied from `sync.Mutex`, with only minor
changes to facilitate FIFO.

However, unless you need the FIFO behavior, you should prefer `sync.Mutex` ,
because its "greedy-relock" behavior requires less context switching and
yields better performance for typical workloads.

## Benchmarks

`fifomu` is generally about 4x slower than stdlib, with two
standout results:

- `BenchmarkMutexUncontended`: `fifomu` is slightly slower than stdlib.
- `BenchmarkMutexSpin`: `fifomu` is ~10x slower than stdlib.

Also, `fifomu` always allocates, while that's not true for stdlib.

```
BenchmarkMutexUncontended
BenchmarkMutexUncontended/fifomu
BenchmarkMutexUncontended/fifomu-10         	33528918	        36.96 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexUncontended/stdlib
BenchmarkMutexUncontended/stdlib-10       	40751988	        41.41 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutex
BenchmarkMutex/fifomu
BenchmarkMutex/fifomu-10                    	 2849842	       422.9 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutex/stdlib
BenchmarkMutex/stdlib-10                  	10844202	       113.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexSlack
BenchmarkMutexSlack/fifomu
BenchmarkMutexSlack/fifomu-10               	 2772765	       432.6 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexSlack/stdlib
BenchmarkMutexSlack/stdlib-10             	11443688	       103.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexWork
BenchmarkMutexWork/fifomu
BenchmarkMutexWork/fifomu-10                	 2767977	       426.9 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexWork/stdlib
BenchmarkMutexWork/stdlib-10              	10525603	       119.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexWorkSlack
BenchmarkMutexWorkSlack/fifomu
BenchmarkMutexWorkSlack/fifomu-10           	 2673496	       482.6 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexWorkSlack/stdlib
BenchmarkMutexWorkSlack/stdlib-10         	10640486	       117.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexNoSpin
BenchmarkMutexNoSpin/fifomu
BenchmarkMutexNoSpin/fifomu-10              	 2387011	       500.9 ns/op	      20 B/op	       1 allocs/op
BenchmarkMutexNoSpin/stdlib
BenchmarkMutexNoSpin/stdlib-10            	 9802332	       135.0 ns/op	      12 B/op	       0 allocs/op
BenchmarkMutexSpin
BenchmarkMutexSpin/fifomu
BenchmarkMutexSpin/fifomu-10                	  436791	       2681 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexSpin/stdlib
BenchmarkMutexSpin/stdlib-10              	 5176197	       229.1 ns/op	       0 B/op	       0 allocs/op
```
