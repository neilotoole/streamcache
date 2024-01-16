package lockqueue

import (
	"context"
	"github.com/oleiade/lane/v2"
	"sync"
)

type oldQueue struct {
	locked   bool
	mu       *sync.Mutex
	q        *lane.Queue[*request]
	muCh     chan struct{}
	notifyCh chan struct{}
}

func newOldQueue(ctx context.Context) *oldQueue {
	q := &oldQueue{
		mu:       &sync.Mutex{},
		q:        lane.NewQueue[*request](),
		notifyCh: make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-q.notifyCh:
				req, ok := q.q.Dequeue()
				if !ok {
					panic("queue is empty")
				}

				// The mu has been unlocked
				<-q.muCh
				q.mu.Lock()
				//req.ch <- q.release
				req.ch <- q.release

				//case mu := <-q.muCh:
				//	mu.Lock()
			}
		}
	}()

	return q
}

func (q *oldQueue) release() {
	q.mu.Unlock()
	q.muCh <- struct{}{}
}



func (q *oldQueue) Request(ctx context.Context) (Unlock, error) {
	req := newRequest("huz")
	q.q.Enqueue(req)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case unlock := <-req.ch:
		return unlock, nil
	}
}
