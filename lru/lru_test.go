package lru

import (
	"github.com/QuangTung97/espresso/allocator"
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestNewLRU(t *testing.T) {
	data := make([]uint64, 1<<12)
	var buddy allocator.Buddy
	allocator.BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := allocator.NewRealSlab(&buddy, 1000, 12)

	l := New(slab, 100)

	assert.Equal(t, slab, l.slab)
	assert.Equal(t, uint32(100), l.Limit())
	assert.Equal(t, uint32(0), l.size)
	assert.Equal(t, nullPtr, l.next)
	assert.Equal(t, nullPtr, l.prev)

	l.UpdateLimit(200)
	assert.Equal(t, uint32(200), l.Limit())
}

func TestLRU_Put_Delete(t *testing.T) {
	data := make([]uint64, 1<<12)
	var buddy allocator.Buddy
	allocator.BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := allocator.NewRealSlab(&buddy, 1000, 12)

	l := New(slab, 100)

	p1, ok := l.Put(2233)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, []uint64{2233}, l.GetLRUList())
	addr, hash := l.Last()
	assert.Equal(t, p1, addr)
	assert.Equal(t, uint64(2233), hash)
	assert.Equal(t, uint32(1), l.Size())

	p2, ok := l.Put(3300)
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)
	assert.Equal(t, []uint64{3300, 2233}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p1, addr)
	assert.Equal(t, uint64(2233), hash)
	assert.Equal(t, uint32(2), l.Size())

	p3, ok := l.Put(4400)
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)
	assert.Equal(t, []uint64{4400, 3300, 2233}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p1, addr)
	assert.Equal(t, uint64(2233), hash)
	assert.Equal(t, uint32(3), l.Size())

	l.Delete(p2)
	assert.Equal(t, []uint64{4400, 2233}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p1, addr)
	assert.Equal(t, uint64(2233), hash)
	assert.Equal(t, uint32(2), l.Size())

	l.Delete(p1)
	assert.Equal(t, []uint64{4400}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p3, addr)
	assert.Equal(t, uint64(4400), hash)
	assert.Equal(t, uint32(1), l.Size())

	l.Delete(p3)
	assert.Equal(t, []uint64(nil), l.GetLRUList())
	assert.Equal(t, uint32(0), l.Size())
}

func TestLRU_Put_Limited(t *testing.T) {
	data := make([]uint64, 1<<12)
	var buddy allocator.Buddy
	allocator.BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := allocator.NewRealSlab(&buddy, 1000, 12)

	l := New(slab, 3)
	p1, ok := l.Put(1100)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)

	p2, ok := l.Put(2200)
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)

	p3, ok := l.Put(3300)
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)

	p4, ok := l.Put(4400)
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p4)

	p5, ok := l.Put(4400)
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p5)
	assert.Equal(t, uint32(3), l.Size())

	l.UpdateLimit(2)

	p6, ok := l.Put(4400)
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p6)
	assert.Equal(t, uint32(3), l.Size())
}

func TestLRU_Put_CanNot_Allocate(t *testing.T) {
	data := make([]uint64, 1<<12)
	var buddy allocator.Buddy
	allocator.BuddyInit(&buddy, 12, 1, unsafe.Pointer(&data[0]))

	slab := allocator.NewRealSlab(&buddy, 1000, 12)

	l := New(slab, 100)
	p1, ok := l.Put(1100)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)

	p2, ok := l.Put(2200)
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)

	p3, ok := l.Put(3300)
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)

	p4, ok := l.Put(4400)
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)

	p5, ok := l.Put(4400)
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p5)
}

func TestLRU_Put_Touch(t *testing.T) {
	data := make([]uint64, 1<<12)
	var buddy allocator.Buddy
	allocator.BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := allocator.NewRealSlab(&buddy, 1000, 12)

	l := New(slab, 100)

	p1, ok := l.Put(2233)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)

	p2, ok := l.Put(3300)
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)

	p3, ok := l.Put(4400)
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)

	p4, ok := l.Put(5500)
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)
	assert.Equal(t, uint32(4), l.Size())

	assert.Equal(t, []uint64{5500, 4400, 3300, 2233}, l.GetLRUList())

	l.Touch(p2)
	assert.Equal(t, []uint64{3300, 5500, 4400, 2233}, l.GetLRUList())
	addr, hash := l.Last()
	assert.Equal(t, p1, addr)
	assert.Equal(t, uint64(2233), hash)

	l.Touch(p1)
	assert.Equal(t, []uint64{2233, 3300, 5500, 4400}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p3, addr)
	assert.Equal(t, uint64(4400), hash)

	l.Touch(p1)
	assert.Equal(t, []uint64{2233, 3300, 5500, 4400}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p3, addr)
	assert.Equal(t, uint64(4400), hash)

	l.Delete(p2)
	l.Delete(p1)
	l.Delete(p3)

	assert.Equal(t, []uint64{5500}, l.GetLRUList())
	l.Touch(p4)
	assert.Equal(t, []uint64{5500}, l.GetLRUList())
	addr, hash = l.Last()
	assert.Equal(t, p4, addr)
	assert.Equal(t, uint64(5500), hash)
}
