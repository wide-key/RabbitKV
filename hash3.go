package rabbitkv

import (
	"math/bits"
)

const (
	MinAddrBits = 4
	Found = 1
	NotFoundAndCanInsert = 2
	NotFoundAndFull = 3
)

type KV32 struct {
	Key   uint32
	Value uint32
}

func (e *KV32) IsValid() bool {
	return e.Value != 0
}

func (e *KV32) Invalidate() bool {
	return e.Value == 0
}

type L1Bucket = [8]KV32
type L2Bucket = [16]KV32
type L3Bucket = [32]KV32

type Hash3 struct {
	addrBits uint32
	addrMask uint32
	buc1     []L1Bucket
	buc2     []L2Bucket
	buc3     []L3Bucket
}

func NewHash3(addrBits uint32) *Hash3 {
	if addrBits < MinAddrBits {
		panic("Too few bits")
	}
	length := 1 << addrBits
	return &Hash3{
		addrBits: uint32(addrBits),
		addrMask: uint32(length-1),
		buc1: make([]L1Bucket, length),
		buc2: make([]L2Bucket, length/4),
		buc3: make([]L3Bucket, length/16),
	}
}

func (h *Hash3) getSlices(key uint32) [3][]KV32 {
	idx1 := key & h.addrMask
	idx2 := bits.Reverse32(key) & h.addrMask // another hash method
	idx3 := (bits.Reverse32(key) + key) & h.addrMask //yet another hash method
	return [3][]KV32 {
		h.buc1[idx1][:],
		h.buc2[idx2/4][:],
		h.buc3[idx3/16][:],
	}
}

// return a matched entry or nil (no found)
func (h *Hash3) Find(key uint32) *KV32 {
	slices := h.getSlices(key)
	for _, slice := range slices {
		for i, e := range slice {
			if e.IsValid() && e.Key == key {
				return &(slice[i])
			}
		}
	}
	return nil
}

// return a matched entry or an empty entry(can be inserted here) or nil (no found and no empty entry)
func (h *Hash3) FindX(key uint32) (*KV32, int) {
	slices := h.getSlices(key)
	for _, slice := range slices {
		for i, e := range slice {
			if e.IsValid() && e.Key == key {
				return &(slice[i]), Found
			}
		}
	}
	for _, slice := range slices {
		for i, e := range slice {
			if !e.IsValid() {
				return &(slice[i]), NotFoundAndCanInsert
			}
		}
	}
	return &(slices[0][0]), NotFoundAndFull
}

func (h *Hash3) Scan(fn func(key uint32, value uint32) ) {
	for _, buc := range h.buc1 {
		for _, e := range buc {
			if e.IsValid() {
				fn(e.Key, e.Value)
			}
		}
	}
}

func (h *Hash3) InitFrom(other *Hash3) {
	other.Scan(func(key uint32, value uint32) {
		kv32, status := h.FindX(key)
		if status == Found {
			panic("Duplicated key")
		}
		if status == NotFoundAndFull {
			panic("No more space")
		}
		kv32.Value = value
	})
}

type Hash3Bundle struct {
	arr  [256]*Hash3
}

func NewHash3Bundle(allAddrBits [256]byte) (hb Hash3Bundle) {
	for i := range hb.arr {
		hb.arr[i] = NewHash3(uint32(allAddrBits[i]))
	}
	return
}

func (hb *Hash3Bundle) GetAllAddrBits() (res [256]byte) {
	for i := range res {
		res[i] = byte(hb.arr[i].addrBits)
	}
	return
}

func (hb *Hash3Bundle) Find(key uint64) *KV32 {
	pos := int(key>>56)
	return hb.arr[pos].Find(uint32(key))
}

func (hb *Hash3Bundle) FindX(key uint64) (*KV32, int) {
	pos := int(key>>56)
	return hb.arr[pos].FindX(uint32(key))
}

func (hb *Hash3Bundle) EnlargeForKey(key uint64) {
	pos := int(key>>56)
	old := hb.arr[pos]
	hb.arr[pos] = NewHash3(old.addrBits+1)
	hb.arr[pos].InitFrom(old)
}

func (hb *Hash3Bundle) EstimatedCount() int64 {
	count := float64(0)
	for i := range hb.arr {
		n := (1<<hb.arr[i].addrBits)*(8+6)
		count += float64(n)*0.75
	}
	return int64(count)
}

func (hb *Hash3Bundle) Set(key uint64, value uint32) {
	kv32, status := hb.FindX(key)
	if status == NotFoundAndFull {
		hb.EnlargeForKey(key)
		hb.Set(key, value)
	} else /*NotFoundAndCanInsert or Found*/{
		kv32.Value = value
	}
}
