package allocator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

func TestBuddyInitSmallMemory(t *testing.T) {
	data := make([]uint64, 1<<14)
	var buddy Buddy
	BuddyInit(&buddy, 12, 14, unsafe.Pointer(&data[0]))

	assert.Equal(t, 1, len(buddy.bitset))
}

func TestBuddyBitSet(t *testing.T) {
	data := make([]uint64, 1<<14)
	var buddy Buddy
	BuddyInit(&buddy, 12, 20, unsafe.Pointer(&data[0]))

	var bitset []uint64

	bitset = []uint64{1, 0, 0, 0}
	assert.Equal(t, bitset, buddy.bitset)

	buddy.clearBit(0)
	bitset = []uint64{0, 0, 0, 0}
	assert.Equal(t, bitset, buddy.bitset)

	buddy.setBit(2 << 12)
	bitset = []uint64{4, 0, 0, 0}
	assert.Equal(t, bitset, buddy.bitset)
	assert.True(t, buddy.isBitSet(2<<12))
	assert.False(t, buddy.isBitSet(3<<12))
}

func TestBuddyInit(t *testing.T) {
	data := make([]uint64, 1<<14)
	var buddy Buddy

	BuddyInit(&buddy, 12, 20, unsafe.Pointer(&data[0]))

	assert.Equal(t, uint32(12), buddy.minSize)
	assert.Equal(t, uint32(20), buddy.maxSize)
	assert.Equal(t, unsafe.Pointer(&data[0]), buddy.data)
	assert.Equal(t, (1<<8)>>6, len(buddy.bitset))

	expected := []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}
	assert.Equal(t, expected, buddy.buckets)
	assert.Equal(t, []uint64{1, 0, 0, 0}, buddy.bitset)
}

func TestBuddyAllocateFromInit(t *testing.T) {
	table := []struct {
		name            string
		size            uint32
		expectedAddr    uint32
		expectedBuckets []uint32
		expectedBitset  []uint64
	}{
		{
			name:         "max",
			size:         20,
			expectedAddr: 0,
			expectedBuckets: []uint32{
				buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
				buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
				buddyNullPtr,
			},
			expectedBitset: []uint64{0, 0, 0, 0},
		},
		{
			name:         "middle",
			size:         18,
			expectedAddr: 0,
			expectedBuckets: []uint32{
				buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
				buddyNullPtr, buddyNullPtr, 1 << 18, 1 << 19,
				buddyNullPtr,
			},
			expectedBitset: []uint64{0, 1, 1, 0},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			data := make([]uint64, 1<<14)
			var b Buddy
			dataPtr := unsafe.Pointer(&data[0])
			BuddyInit(&b, 12, 20, dataPtr)

			p, ok := b.Allocate(e.size)
			assert.True(t, ok)

			assert.Equal(t, e.expectedAddr, p)
			assert.Equal(t, e.expectedBuckets, b.buckets)
			assert.Equal(t, e.expectedBitset, b.bitset)
		})
	}
}

func TestComputeRootAndNeighborAddr(t *testing.T) {
	rootAddr, neighborAddr := computeRootAndNeighborAddr(1<<19+1<<18, 18)
	assert.Equal(t, uint32(1<<19), rootAddr)
	assert.Equal(t, uint32(1<<19), neighborAddr)

	rootAddr, neighborAddr = computeRootAndNeighborAddr(1<<19, 17)
	assert.Equal(t, uint32(1<<19), rootAddr)
	assert.Equal(t, uint32(1<<19+1<<17), neighborAddr)
}

func TestBuddyAllocateDeallocate1(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	var expectedBuckets []uint32

	p, ok := b.Allocate(20)
	assert.True(t, ok)
	b.Deallocate(p, 20)

	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}
	assert.Equal(t, expectedBuckets, b.buckets)

	p1, _ := b.Allocate(19)
	p2, _ := b.Allocate(18)
	assert.Equal(t, uint32(0), p1)
	assert.Equal(t, uint32(1<<19), p2)

	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, 1<<19 + 1<<18, buddyNullPtr,
		buddyNullPtr,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{0, 0, 0, 1}, b.bitset)

	b.Deallocate(p2, 18)

	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, 1 << 19,
		buddyNullPtr,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{0, 0, 1, 0}, b.bitset)

	b.Deallocate(p1, 19)
	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{1, 0, 0, 0}, b.bitset)
}

func TestBuddyAllocateDeallocate2(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	var expectedBuckets []uint32

	p, ok := b.Allocate(17)
	assert.True(t, ok)
	assert.Equal(t, uint32(0), p)

	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, 1 << 17, 1 << 18, 1 << 19,
		buddyNullPtr,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{0x100000000, 1, 1, 0}, b.bitset)

	b.Deallocate(p, 17)
	expectedBuckets = []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{1, 0, 0, 0}, b.bitset)
}

func TestBuddyAllocateDeallocate_NoRemain(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	b.Allocate(19)
	b.Allocate(19)
	p, ok := b.Allocate(19)
	assert.Equal(t, uint32(0), p)
	assert.False(t, ok)
}

func TestBuddyAllocateDeallocate3(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	p1, _ := b.Allocate(19)
	p2, _ := b.Allocate(18)

	b.Deallocate(p1, 19)
	p3, _ := b.Allocate(18)

	assert.Equal(t, uint32(1<<19), p2)
	assert.Equal(t, uint32(1<<19+1<<18), p3)

	b.Deallocate(p2, 18)
	b.Deallocate(p3, 18)

	expectedBuckets := []uint32{
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		buddyNullPtr, buddyNullPtr, buddyNullPtr, buddyNullPtr,
		0,
	}
	assert.Equal(t, expectedBuckets, b.buckets)
	assert.Equal(t, []uint64{1, 0, 0, 0}, b.bitset)
}

func TestBuddyAllocateDeallocate4(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	p1, _ := b.Allocate(19)
	p2, _ := b.Allocate(18)

	p3, _ := b.Allocate(18)

	assert.Equal(t, uint32(1<<19), p2)
	assert.Equal(t, uint32(1<<19+1<<18), p3)

	b.Deallocate(p1, 19)

	p4, _ := b.Allocate(18)
	p5, _ := b.Allocate(17)
	p6, _ := b.Allocate(17)
	p7, ok := b.Allocate(18)

	assert.False(t, ok)

	assert.Equal(t, uint32(0), p4)
	assert.Equal(t, uint32(1<<18), p5)
	assert.Equal(t, uint32(1<<18+1<<17), p6)
	assert.Equal(t, uint32(0), p7)

	assert.Equal(t, []uint32(nil), b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p6, 17)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p3, 18)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32{1<<19 + 1<<18}, b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p4, 18)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32{0, 1<<19 + 1<<18}, b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p2, 18)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32{0}, b.contentOfList(18))
	assert.Equal(t, []uint32{1 << 19}, b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p5, 17)
	assert.Equal(t, []uint32(nil), b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32{0}, b.contentOfList(20))
	assert.Equal(t, []uint64{1, 0, 0, 0}, b.bitset)
}

func TestBuddyAllocateDeallocate5(t *testing.T) {
	data := make([]uint64, 1<<14)
	var b Buddy
	dataPtr := unsafe.Pointer(&data[0])
	BuddyInit(&b, 12, 20, dataPtr)

	p1, _ := b.Allocate(19)
	p2, _ := b.Allocate(18)

	p3, _ := b.Allocate(18)

	assert.Equal(t, uint32(1<<19), p2)
	assert.Equal(t, uint32(1<<19+1<<18), p3)

	b.Deallocate(p1, 19)

	p4, _ := b.Allocate(18)
	p5, _ := b.Allocate(17)
	p6, _ := b.Allocate(17)
	p7, ok := b.Allocate(18)

	assert.False(t, ok)

	assert.Equal(t, uint32(0), p4)
	assert.Equal(t, uint32(1<<18), p5)
	assert.Equal(t, uint32(1<<18+1<<17), p6)
	assert.Equal(t, uint32(0), p7)

	assert.Equal(t, []uint32(nil), b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p6, 17)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p3, 18)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32{1<<19 + 1<<18}, b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p4, 18)
	assert.Equal(t, []uint32{1<<18 + 1<<17}, b.contentOfList(17))
	assert.Equal(t, []uint32{0, 1<<19 + 1<<18}, b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p5, 17)
	assert.Equal(t, []uint32(nil), b.contentOfList(17))
	assert.Equal(t, []uint32{1<<19 + 1<<18}, b.contentOfList(18))
	assert.Equal(t, []uint32{0}, b.contentOfList(19))
	assert.Equal(t, []uint32(nil), b.contentOfList(20))

	b.Deallocate(p2, 18)
	assert.Equal(t, []uint32(nil), b.contentOfList(17))
	assert.Equal(t, []uint32(nil), b.contentOfList(18))
	assert.Equal(t, []uint32(nil), b.contentOfList(19))
	assert.Equal(t, []uint32{0}, b.contentOfList(20))
	assert.Equal(t, []uint64{1, 0, 0, 0}, b.bitset)
}

func BenchmarkBuddy_Allocate(b *testing.B) {
	for n := 0; n < b.N; n++ {
		data := make([]uint64, 1<<14)
		var buddy Buddy
		dataPtr := unsafe.Pointer(&data[0])
		BuddyInit(&buddy, 12, 20, dataPtr)

		for i := 0; i < 1000000; i++ {
			p, _ := buddy.Allocate(16)
			buddy.Deallocate(p, 16)
		}
	}
}
