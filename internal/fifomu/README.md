# fifomu: mutex with FIFO lock acquisition

Package `fifomu` provides a `Mutex` whose `Lock` method returns
the lock to callers in FIFO order. This is unlike `sync.Mutex`, where a
single goroutine can repeatedly lock and unlock and relock the mutex
without handing off to other lock waiter goroutines (until after a 1ms
starvation threshold, at which point `sync.Mutex` enters "starvation mode"
for those starved waiters, but that's too late for our use case).

`fifomu.Mutex` implements the exported methods of `sync.Mutex` and thus is
a drop-in replacement (and by extension also implements `sync.Locker`).

Note: unless you need the FIFO behavior, you should prefer `sync.Mutex`,
because, for typical workloads, its "greedy-relock" behavior requires
less goroutine switching and yields better performance.
