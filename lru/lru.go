package lru

import (
	"github.com/QuangTung97/espresso/allocator"
	"math"
)

const nullPtr uint32 = math.MaxUint32

// LRU ...
type LRU struct {
	slab  *allocator.RealSlab
	limit uint32

	next uint32
	prev uint32
	size uint32
}

// ListHead ...
type ListHead struct {
	next uint32
	prev uint32
	hash uint64
}

// New ...
func New(slab *allocator.RealSlab, limit uint32) *LRU {
	return &LRU{
		slab:  slab,
		limit: limit,

		next: nullPtr,
		prev: nullPtr,
		size: 0,
	}
}

// GetLRUList ...
func (l *LRU) GetLRUList() []uint64 {
	var result []uint64
	n := l.next
	for n != nullPtr {
		head := (*ListHead)(l.slab.ToRealAddr(n))
		result = append(result, head.hash)
		n = head.next
	}
	return result
}

// Put ...
func (l *LRU) Put(hash uint64) (uint32, bool) {
	if l.size >= l.limit {
		return 0, false
	}

	addr, ok := l.slab.Allocate()
	if !ok {
		return 0, false
	}

	l.size++
	head := (*ListHead)(l.slab.ToRealAddr(addr))
	head.hash = hash

	if l.next != nullPtr {
		next := (*ListHead)(l.slab.ToRealAddr(l.next))
		next.prev = addr
	} else {
		l.prev = addr
	}

	head.next = l.next
	head.prev = nullPtr
	l.next = addr

	return addr, true
}

// Last ...
func (l *LRU) Last() (uint32, uint64) {
	last := (*ListHead)(l.slab.ToRealAddr(l.prev))
	return l.prev, last.hash
}

// Delete ...
func (l *LRU) Delete(addr uint32) {
	l.size--
	head := (*ListHead)(l.slab.ToRealAddr(addr))

	if head.next != nullPtr {
		next := (*ListHead)(l.slab.ToRealAddr(head.next))
		next.prev = head.prev
	} else {
		l.prev = head.prev
	}

	if head.prev != nullPtr {
		prev := (*ListHead)(l.slab.ToRealAddr(head.prev))
		prev.next = head.next
	} else {
		l.next = head.next
	}
}

// Touch ...
func (l *LRU) Touch(addr uint32) {
	// Delete
	head := (*ListHead)(l.slab.ToRealAddr(addr))

	if head.next != nullPtr {
		next := (*ListHead)(l.slab.ToRealAddr(head.next))
		next.prev = head.prev
	} else {
		l.prev = head.prev
	}

	if head.prev != nullPtr {
		prev := (*ListHead)(l.slab.ToRealAddr(head.prev))
		prev.next = head.next
	} else {
		l.next = head.next
	}

	// Insert
	if l.next != nullPtr {
		next := (*ListHead)(l.slab.ToRealAddr(l.next))
		next.prev = addr
	} else {
		l.prev = addr
	}

	head.next = l.next
	head.prev = nullPtr
	l.next = addr
}

// Size ...
func (l *LRU) Size() uint32 {
	return l.size
}

// Limit ...
func (l *LRU) Limit() uint32 {
	return l.limit
}

// UpdateLimit ...
func (l *LRU) UpdateLimit(newLimit uint32) {
	l.limit = newLimit
}
