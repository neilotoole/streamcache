# muqu: fifo-queue mutex

Package muqu provides a `Mutex` that uses a FIFO queue to
ensure fairness. Mutex implements `sync.Locker`, so it can
be used interchangeably with `sync.Mutex`. Performance is
significantly worse than `sync.Mutex`, so it should only be
used when fairness is required.

Note that `sync.Mutex`'s implementation discusses mutex
fairness, where the `sync.Mutex` can be in 2 modes of operations:
normal and starvation. Starvation mode kicks in when a waiter
fails to acquire the mutex for more than 1ms. However, for
some purposes waiting 1ms simply doesn't work, and FIFO
fairness is required, hence the need for this package.

## Benchmarks

`muqu` is generally about 4x slower than stdlib, with two
standout results:

- `BenchmarkMutexUncontended`: `muqu` is slightly slower than stdlib.
- `BenchmarkMutexSpin`: `muqu` is ~10x slower than stdlib.

Also, `muqu` always allocates, while that's not true for stdlib.

```
BenchmarkMutexUncontended
BenchmarkMutexUncontended/muqu
BenchmarkMutexUncontended/muqu-10         	33528918	        36.96 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexUncontended/stdlib
BenchmarkMutexUncontended/stdlib-10       	40751988	        41.41 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutex
BenchmarkMutex/muqu
BenchmarkMutex/muqu-10                    	 2849842	       422.9 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutex/stdlib
BenchmarkMutex/stdlib-10                  	10844202	       113.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexSlack
BenchmarkMutexSlack/muqu
BenchmarkMutexSlack/muqu-10               	 2772765	       432.6 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexSlack/stdlib
BenchmarkMutexSlack/stdlib-10             	11443688	       103.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexWork
BenchmarkMutexWork/muqu
BenchmarkMutexWork/muqu-10                	 2767977	       426.9 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexWork/stdlib
BenchmarkMutexWork/stdlib-10              	10525603	       119.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexWorkSlack
BenchmarkMutexWorkSlack/muqu
BenchmarkMutexWorkSlack/muqu-10           	 2673496	       482.6 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexWorkSlack/stdlib
BenchmarkMutexWorkSlack/stdlib-10         	10640486	       117.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkMutexNoSpin
BenchmarkMutexNoSpin/muqu
BenchmarkMutexNoSpin/muqu-10              	 2387011	       500.9 ns/op	      20 B/op	       1 allocs/op
BenchmarkMutexNoSpin/stdlib
BenchmarkMutexNoSpin/stdlib-10            	 9802332	       135.0 ns/op	      12 B/op	       0 allocs/op
BenchmarkMutexSpin
BenchmarkMutexSpin/muqu
BenchmarkMutexSpin/muqu-10                	  436791	       2681 ns/op	      32 B/op	       1 allocs/op
BenchmarkMutexSpin/stdlib
BenchmarkMutexSpin/stdlib-10              	 5176197	       229.1 ns/op	       0 B/op	       0 allocs/op
```
