package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestSlab_Init(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 100, 12)
	assert.Equal(t, &buddy, slab.buddy)
	assert.Equal(t, uint32(100), slab.elemSize)
	assert.Equal(t, uint32(12), slab.chunkSizeLog)
	assert.Equal(t, uint32((1<<12)/100), slab.numElemPerChunk)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)
}

func TestSlab_Allocate_Deallocate(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 128, 12)

	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, uint32(1), slab.freeListIndex)

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(128), p2)
	assert.Equal(t, uint32(2), slab.freeListIndex)

	slab.SetElem(p2, []byte{1, 2, 3, 4})

	movedAddr, needMove := slab.Deallocate(p1)
	assert.Equal(t, uint32(1), slab.freeListIndex)
	assert.True(t, needMove)
	assert.Equal(t, uint32(128), movedAddr)

	elem := slab.GetElem(p1)
	assert.Equal(t, 128, len(elem))
	assert.Equal(t, []byte{1, 2, 3, 4}, elem[:4])

	movedAddr, needMove = slab.Deallocate(p1)
	assert.Equal(t, uint32(0), slab.freeListIndex)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
}

func TestSlab_Allocate_Deallocate2(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 1000, 12)

	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, uint32(1), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)
	assert.Equal(t, uint32(2), slab.freeListIndex)

	p3, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)
	assert.Equal(t, uint32(3), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	p4, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)

	movedAddr, needMove := slab.Deallocate(p2)
	assert.True(t, needMove)
	assert.Equal(t, p4, movedAddr)

	movedAddr, needMove = slab.Deallocate(p1)
	assert.True(t, needMove)
	assert.Equal(t, p3, movedAddr)

	movedAddr, needMove = slab.Deallocate(p2)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint32(1), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	movedAddr, needMove = slab.Deallocate(p1)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)

	assert.Equal(t, []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}, buddy.buckets)
}

func TestSlab_Allocate_Deallocate3(t *testing.T) {
	data := make([]uint64, 1<<17)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<8, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 1000, 12)

	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, uint32(1), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1000), p2)
	assert.Equal(t, uint32(2), slab.freeListIndex)

	p3, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(2000), p3)
	assert.Equal(t, uint32(3), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	p4, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(3000), p4)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)

	p5, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12), p5)
	assert.Equal(t, uint32(1<<12), slab.currentChunkAddr)
	assert.Equal(t, uint32(1), slab.freeListIndex)

	movedAddr, needMove := slab.Deallocate(p2)
	assert.True(t, needMove)
	assert.Equal(t, p5, movedAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)

	movedAddr, needMove = slab.Deallocate(p1)
	assert.True(t, needMove)
	assert.Equal(t, p4, movedAddr)
	assert.Equal(t, uint32(3), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	movedAddr, needMove = slab.Deallocate(p2)
	assert.True(t, needMove)
	assert.Equal(t, p3, movedAddr)
	assert.Equal(t, uint32(2), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	movedAddr, needMove = slab.Deallocate(p2)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint32(1), slab.freeListIndex)
	assert.Equal(t, uint32(0), slab.currentChunkAddr)

	movedAddr, needMove = slab.Deallocate(p1)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), movedAddr)
	assert.Equal(t, uint32(0), slab.freeListIndex)
	assert.Equal(t, buddyNullPtr, slab.currentChunkAddr)

	assert.Equal(t, []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}, buddy.buckets)
}

func TestSlab_Allocate_Full(t *testing.T) {
	data := make([]uint64, 1<<10)
	var buddy Buddy
	BuddyInit(&buddy, 12, 2, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 4000, 12)

	p1, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p1)

	p2, ok := slab.Allocate()
	assert.True(t, ok)
	assert.Equal(t, uint32(1<<12), p2)

	p3, ok := slab.Allocate()
	assert.False(t, ok)
	assert.Equal(t, uint32(0), p3)

	assert.Equal(t, []uint32{
		buddyNullPtr, buddyNullPtr,
	}, buddy.buckets)

	moved, needMove := slab.Deallocate(p1)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), moved)

	moved, needMove = slab.Deallocate(p2)
	assert.False(t, needMove)
	assert.Equal(t, uint32(0), moved)

	assert.Equal(t, []uint32{
		buddyNullPtr, 0,
	}, buddy.buckets)
}

func BenchmarkSlab_Allocate_Deallocate_Interact_Buddy(b *testing.B) {
	data := make([]uint64, 1<<20)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<11, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 100, 12)
	for n := 0; n < b.N; n++ {
		p1, _ := slab.Allocate()
		slab.Deallocate(p1)
	}
}

func BenchmarkSlab_Allocate_Deallocate_Not_Interact_Buddy(b *testing.B) {
	data := make([]uint64, 1<<20)
	var buddy Buddy
	BuddyInit(&buddy, 12, 1<<11, unsafe.Pointer(&data[0]))

	slab := NewSlab(&buddy, 100, 12)
	slab.Allocate()

	for n := 0; n < b.N; n++ {
		p1, _ := slab.Allocate()
		slab.Deallocate(p1)
	}
}
