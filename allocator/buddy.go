package allocator

import (
	"math"
	"unsafe"
)

const (
	buddyNullPtr uint32 = math.MaxUint32
)

// Buddy ...
type Buddy struct {
	minSize uint32
	maxSize uint32
	data    unsafe.Pointer
	buckets []uint32
	bitset  []uint64
}

type buddyListHead struct {
	next         uint32
	prev         uint32
	bucketOffset uint32
}

// BuddyInit ...
func BuddyInit(b *Buddy, minSizeLog uint32, maxSizeLog uint32, data unsafe.Pointer) {
	b.minSize = minSizeLog
	b.maxSize = maxSizeLog
	b.data = data
	b.buckets = make([]uint32, maxSizeLog-minSizeLog+1)

	last := maxSizeLog - minSizeLog

	if last < 6 {
		b.bitset = make([]uint64, 1)
	} else {
		b.bitset = make([]uint64, 1<<(last-6))
	}

	for i := uint32(0); i < last; i++ {
		b.buckets[i] = buddyNullPtr
	}
	b.buckets[last] = 0
	b.bitset[0] = 1

	header := (*buddyListHead)(b.data)
	header.next = buddyNullPtr
	header.prev = buddyNullPtr
}

func (b *Buddy) setBit(addr uint32) {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := uint64(1 << pos)
	b.bitset[index>>6] |= mask
}

func (b *Buddy) clearBit(addr uint32) {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := ^uint64(1 << pos)
	b.bitset[index>>6] &= mask
}

func (b *Buddy) isBitSet(addr uint32) bool {
	index := addr >> b.minSize
	pos := index & 0x3f
	mask := uint64(1 << pos)
	return b.bitset[index>>6]&mask != 0
}

func buddyAddListHead(data unsafe.Pointer, root *uint32, offset uint32, node *buddyListHead) {
	nodeAddr := uint32(uintptr(unsafe.Pointer(node)) - uintptr(data))
	if *root != buddyNullPtr {
		next := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(*root)))
		next.prev = nodeAddr
	}

	node.next = *root
	node.prev = buddyNullPtr
	node.bucketOffset = offset
	*root = nodeAddr
}

func buddyRemoveListHead(data unsafe.Pointer, root *uint32, node *buddyListHead) {
	if node.next != buddyNullPtr {
		next := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(node.next)))
		next.prev = node.prev
	}

	if node.prev != buddyNullPtr {
		prev := (*buddyListHead)(unsafe.Pointer(uintptr(data) + uintptr(node.prev)))
		prev.next = node.next
	} else {
		*root = node.next
	}
}

func (b *Buddy) contentOfList(order uint32) []uint32 {
	var result []uint32
	offset := order - b.minSize

	addr := b.buckets[offset]
	for addr != buddyNullPtr {
		node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addr)))
		if node.bucketOffset == offset {
			result = append(result, addr)
		}

		addr = node.next
	}

	return result
}

// Allocate ...
func (b *Buddy) Allocate(sizeLog uint32) uintptr {
	offset := sizeLog - b.minSize
	maxOffset := b.maxSize - b.minSize
	emptyOffset := offset
	for ; emptyOffset <= maxOffset && b.buckets[emptyOffset] == buddyNullPtr; emptyOffset++ {
	}
	if emptyOffset > maxOffset {
		return 0
	}

	addrIndex := b.buckets[emptyOffset]
	header := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addrIndex)))
	b.buckets[emptyOffset] = header.next
	b.clearBit(addrIndex)

	if emptyOffset == offset {
		return uintptr(b.data) + uintptr(addrIndex)
	}

	for i := emptyOffset - 1; i >= offset; i-- {
		p := addrIndex + (1 << (i + b.minSize))
		node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(p)))

		buddyAddListHead(b.data, &b.buckets[i], i, node)
		b.setBit(p)
	}

	return uintptr(b.data) + uintptr(addrIndex)
}

func computeRootAndNeighborAddr(addr uint32, sizeLog uint32) (uint32, uint32) {
	mask := uint32(math.MaxUint32) << (sizeLog + 1)
	maskedAddr := addr & mask
	if maskedAddr == addr {
		return maskedAddr, addr + (1 << sizeLog)
	}
	return maskedAddr, maskedAddr
}

// Deallocate ...
func (b *Buddy) Deallocate(p uintptr, sizeLog uint32) {
	offset := sizeLog - b.minSize
	addr := uint32(p - uintptr(b.data))

	for sizeLog < b.maxSize {
		rootAddr, neighborAddr := computeRootAndNeighborAddr(addr, sizeLog)

		if !b.isBitSet(neighborAddr) {
			break
		}

		neighborHeader := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(neighborAddr)))
		if neighborHeader.bucketOffset != offset {
			break
		}

		buddyRemoveListHead(b.data, &b.buckets[offset], neighborHeader)
		b.clearBit(neighborAddr)

		addr = rootAddr
		sizeLog++
		offset++
	}

	node := (*buddyListHead)(unsafe.Pointer(uintptr(b.data) + uintptr(addr)))
	buddyAddListHead(b.data, &b.buckets[offset], offset, node)
	b.setBit(addr)
}
