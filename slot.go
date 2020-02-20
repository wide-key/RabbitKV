package rabbitkv

import (
	"bytes"
	"errors"
	"encoding/binary"

	"github.com/mmcloughlin/meow"
)

type Pair struct {
	key   []byte
	value []byte
}

type Slot struct {
	pairs []Pair
}

func NewSlot(key, value []byte) Slot {
	return Slot {
		pairs: []Pair {
			{key: key, value: value},
		},
	}
}

func (s *Slot) Empty() bool {
	return len(s.pairs) == 0
}

func (s *Slot) Add(pair Pair) {
	for i := range s.pairs {
		if bytes.Equal(s.pairs[i].key, pair.key) {
			// overwrite an existing value
			s.pairs[i].value = pair.value
			return
		}
	}
	// append to the tail
	s.pairs = append(s.pairs, pair)
}

func (s *Slot) Get(key []byte) []byte {
	for _, pair := range s.pairs {
		if bytes.Equal(pair.key, key) {
			return pair.value
		}
	}
	return nil
}

func (s *Slot) Remove(key []byte) (existed bool) {
	idx := -1
	for i, pair := range s.pairs {
		if bytes.Equal(pair.key, key) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false
	}
	copy(s.pairs[idx:], s.pairs[idx+1:])
	s.pairs = s.pairs[:len(s.pairs)-1]
	return true
}

//total-length, pair-count, lengths-of-kv, payload-of-kv, cksum, padding
func (s *Slot) ToSlicesForDump() ([][]byte, int) {
	hasher := meow.New32(0)
	head := make([]byte, 4, (len(s.pairs)*2+2)*4)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(s.pairs)))
	hasher.Write(buf[:])
	head = append(head, buf[:]...)
	for _, pair := range s.pairs {
		for _, bz := range [][]byte{pair.key, pair.value} {
			binary.LittleEndian.PutUint32(buf[:], uint32(len(bz)))
			hasher.Write(buf[:])
			head = append(head, buf[:]...)
		}
	}
	totalLen := len(head)-4 //exclude the leading 4 bytes

	res := make([][]byte, 0, len(s.pairs)*2+3)
	res = append(res, head)
	for _, pair := range s.pairs {
		for _, bz := range [][]byte{pair.key, pair.value} {
			hasher.Write(bz)
			res = append(res, bz)
			totalLen += len(bz)
		}
	}
	binary.LittleEndian.PutUint32(buf[:], hasher.Sum32())
	res = append(res, buf[:])
	binary.LittleEndian.PutUint32(head[:4], uint32(totalLen))
	rem := (totalLen+4)%16 //include the leading 4 bytes
	if rem == 0 {
		res = append(res, []byte{}) //no padding
	} else {
		res = append(res, make([]byte, 16 - rem)) //padding
		totalLen += 16 - rem
	}
	return res, totalLen
}

func extractBytes(in []byte, length int) (result, out []byte, err error) {
	if len(in) < length {
		return nil, nil, errors.New("Not enough bytes to read")
	}
	return in[:length], in[length:], nil
}

func extractUint32(in []byte) (result uint32, out []byte, err error) {
	if len(in) < 4 {
		return 0, nil, errors.New("Not enough bytes to read")
	}
	return binary.LittleEndian.Uint32(in[:4]), in[4:], nil
}

func BytesToSlot(bzIn []byte) (slot Slot, err error) {
	pairCount, bz, err := extractUint32(bzIn)
	if err != nil {
		return
	}

	lenList := make([]uint32, 2*int(pairCount))
	for i := range lenList {
		lenList[i], bz, err = extractUint32(bz)
		if err != nil {
			return
		}
	}
	slot.pairs = make([]Pair, int(pairCount))
	for i := range slot.pairs {
		slot.pairs[i].key, bz, err = extractBytes(bz, int(lenList[2*i]))
		if err != nil {
			return
		}
		slot.pairs[i].value, bz, err = extractBytes(bz, int(lenList[2*i+1]))
		if err != nil {
			return
		}
	}
	cksum, bz, err := extractUint32(bz)
	if err != nil {
		return
	}

	hasher := meow.New32(0)
	hasher.Write(bzIn[:len(bzIn)-len(bz)/*padding*/-4/*cksum*/])
	if hasher.Sum32() != cksum {
		err = errors.New("Checksum Error")
		return
	}

	return
}

