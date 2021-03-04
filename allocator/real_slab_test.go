package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestNewRealSlab(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewRealSlab(&buddy, 100, 12)
	assert.Equal(t, &buddy, slab.buddy)
	assert.Equal(t, uint32(100), slab.elemSize)
	assert.Equal(t, uint32(12), slab.chunkSizeLog)
	assert.Equal(t, uint32(40), slab.numElemPerChunk)
	assert.Equal(t, uint64(96), slab.unusedBytes)
	assert.Equal(t, uint64(0), slab.memoryUsage)
	assert.Equal(t, buddyNullPtr, slab.freeList)
}

func TestRealSlab_Allocate_Deallocate(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewRealSlab(&buddy, 1000, 12)

	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, []uint32{1000, 2000, 3000}, slab.contentOfList())
	assert.Equal(t, uint64(1000+96), slab.GetMemUsage())

	assert.Equal(t, unsafe.Pointer(&data[0]), slab.ToRealAddr(p1))

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)
	assert.Equal(t, []uint32{2000, 3000}, slab.contentOfList())
	assert.Equal(t, uint64(2*1000+96), slab.GetMemUsage())

	p3, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)
	assert.Equal(t, []uint32{3000}, slab.contentOfList())
	assert.Equal(t, uint64(3*1000+96), slab.GetMemUsage())

	p4, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)
	assert.Equal(t, []uint32(nil), slab.contentOfList())
	assert.Equal(t, uint64(4*1000+96), slab.GetMemUsage())

	p5, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12), p5)
	assert.Equal(t, []uint32{1<<12 + 1000, 1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(5*1000+96*2), slab.GetMemUsage())

	p6, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12+1000), p6)
	assert.Equal(t, []uint32{1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(6*1000+96*2), slab.GetMemUsage())

	slab.Deallocate(p1)
	assert.Equal(t, []uint32{0, 1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(5*1000+96*2), slab.GetMemUsage())

	slab.Deallocate(p5)
	assert.Equal(t, []uint32{1 << 12, 0, 1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(4*1000+96*2), slab.GetMemUsage())

	slab.Deallocate(p2)
	assert.Equal(t, []uint32{1000, 1 << 12, 0, 1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(3*1000+96*2), slab.GetMemUsage())

	p7, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p7)
	assert.Equal(t, []uint32{1 << 12, 0, 1<<12 + 2000, 1<<12 + 3000}, slab.contentOfList())
	assert.Equal(t, uint64(4*1000+96*2), slab.GetMemUsage())
}

func TestRealSlab_Allocate_Deallocate_Exceed_Buddy(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := NewRealSlab(&buddy, 1000, 12)
	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)

	p3, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)

	p4, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)

	p5, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12), p5)

	p6, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12+1000), p6)

	p7, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12+2000), p7)

	p8, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12+3000), p8)

	p9, ok := slab.Allocate()
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p9)
}
