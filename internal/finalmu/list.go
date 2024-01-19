package finalmu

import "sync"

var elementPool = sync.Pool{New: func() any { return new(element[waiter]) }}

// list represents a doubly linked list.
type list[T any] struct {
	root element[T]
	len  uint
}

// Init initializes or clears list l.
func (l *list[T]) Init() *list[T] {
	l.root.next = &l.root
	l.root.prev = &l.root
	l.len = 0

	return l
}

// NewList returns an initialized list.
func newList[T any]() *list[T] {
	return new(list[T]).Init()
}

// Len returns the number of elements of list l.
func (l *list[T]) Len() uint {
	return l.len
}

// Front returns the first element of list l or nil.
func (l *list[T]) Front() *element[T] {
	if l.len == 0 {
		return nil
	}

	return l.root.next
}

// Back returns the last element of list l or nil.
func (l *list[T]) Back() *element[T] {
	if l.len == 0 {
		return nil
	}

	return l.root.prev
}

// PushFront inserts a new element e with value v at
// the front of list l and returns e.
func (l *list[T]) PushFront(v T) *element[T] {
	l.lazyInit()
	return l.insertValue(v, &l.root)
}

// PushBack inserts a new element e with value v at
// the back of list l and returns e.
func (l *list[T]) PushBack(v T) *element[T] {
	l.lazyInit()

	return l.insertValue(v, l.root.prev)
}

// InsertBefore inserts a new element e with value v
// immediately before mark and returns e.
func (l *list[T]) InsertBefore(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}

	return l.insertValue(v, mark.prev)
}

// InsertAfter inserts a new element e with value v
// immediately after mark and returns e.
func (l *list[T]) InsertAfter(v T, mark *element[T]) *element[T] {
	if mark.list != l {
		return nil
	}

	return l.insertValue(v, mark)
}

// Remove removes e from l if e is an element of list l.
func (l *list[T]) Remove(e *element[T]) T {
	if e.list == l {
		l.remove(e)
	}

	v := e.Value
	elementPool.Put(e)
	return v
}

// MoveToFront moves element e to the front of list l.
func (l *list[T]) MoveToFront(e *element[T]) {
	if e.list != l || l.root.next == e {
		return
	}

	l.move(e, &l.root)
}

// MoveToBack moves element e to the back of list l.
func (l *list[T]) MoveToBack(e *element[T]) {
	if e.list != l || l.root.prev == e {
		return
	}

	l.move(e, l.root.prev)
}

// MoveBefore moves element e to its new position before mark.
func (l *list[T]) MoveBefore(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}

	l.move(e, mark.prev)
}

// MoveAfter moves element e to its new position after mark.
func (l *list[T]) MoveAfter(e, mark *element[T]) {
	if e.list != l || e == mark || mark.list != l {
		return
	}

	l.move(e, mark)
}

// PushBackList inserts a copy of an other list at the back of list l.
func (l *list[T]) PushBackList(other *list[T]) {
	l.lazyInit()

	for i, e := other.Len(), other.Front(); i > 0; i, e = i-1, e.Next() {
		l.insertValue(e.Value, l.root.prev)
	}
}

// PushFrontList inserts a copy of an other list at the front of list l.
func (l *list[T]) PushFrontList(other *list[T]) {
	l.lazyInit()
	for i, e := other.Len(), other.Back(); i > 0; i, e = i-1, e.Prev() {
		l.insertValue(e.Value, &l.root)
	}
}

func (l *list[T]) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

func (l *list[T]) insert(e, at *element[T]) *element[T] {
	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e
	e.list = l
	l.len++

	return e
}

func (l *list[T]) insertValue(v T, at *element[T]) *element[T] {
	e := elementPool.Get().(*element[T])
	e.Value = v
	return l.insert(e, at)
}

func (l *list[T]) remove(e *element[T]) *element[T] {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	l.len--
	return e
}

func (l *list[T]) move(e, at *element[T]) *element[T] {
	if e == at {
		return e
	}

	e.prev.next = e.next
	e.next.prev = e.prev

	e.prev = at
	e.next = at.next
	e.prev.next = e
	e.next.prev = e

	return e
}

// element is a node of a linked list.
type element[T any] struct {
	next, prev *element[T]

	list *list[T]

	Value T
}

// Next returns the next list element or nil.
func (e *element[T]) Next() *element[T] {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}

	return nil
}

// Prev returns the previous list element or nil.
func (e *element[T]) Prev() *element[T] {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}

	return nil
}
