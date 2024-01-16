package lockqueue

import "sync"

func newQueue[T any]() *queue[T] {
	return &queue[T]{
		mu:    sync.Mutex{},
		elems: make([]T, 0),
	}
}

type queue[T any] struct {
	mu    sync.Mutex
	elems []T
}

func (q *queue[T]) enqueue(elem T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.elems = append(q.elems, elem)
}

func (q *queue[T]) dequeue() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.elems) == 0 {
		var zero T
		return zero, false
	}
	elem := q.elems[0]
	q.elems = q.elems[1:]
	return elem, true
}
