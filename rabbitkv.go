package rabbitkv

import (
	"errors"
	"os"
	"math"
	"sync"
	"encoding/binary"

	"github.com/mmcloughlin/meow"
)

//TODO gc

const MetaInfoBytes = 256+8*3+1+4

type MetaInfo struct {
	allAddrBits     [256]byte // just hints, they are ok to be incorrect
	nextGcPosition  uint64 // can be less than real value if not closed properly
	activeByteCount uint64 // need to recover if not closed properly
	seed            uint64 // constant during lifetime
	blockSize       uint64 // constant during lifetime
	closed          bool
}

func (mi *MetaInfo) ToBytes() (res [MetaInfoBytes]byte) {
	copy(res[:], mi.allAddrBits[:])
	start, end := 256, 264
	binary.LittleEndian.PutUint64(res[start:end], mi.nextGcPosition)
	start, end = end, end+8
	binary.LittleEndian.PutUint64(res[start:end], mi.activeByteCount)
	start, end = end, end+8
	binary.LittleEndian.PutUint64(res[start:end], mi.seed)
	start, end = end, end+8
	binary.LittleEndian.PutUint64(res[start:end], mi.blockSize)
	res[end] = 0
	if mi.closed {
		res[end] =1
	}
	cksum := meow.Checksum32(0, res[:MetaInfoBytes-4])
	binary.LittleEndian.PutUint32(res[MetaInfoBytes-4:], cksum)
	return
}

func (mi *MetaInfo) FromBytes(bz [MetaInfoBytes]byte) {
	cksum := meow.Checksum32(0, bz[:MetaInfoBytes-4])
	if cksum != binary.LittleEndian.Uint32(bz[MetaInfoBytes-4:]) {
		panic("Checksum Error")
	}
	copy(mi.allAddrBits[:], bz[:256])
	start, end := 256, 264
	mi.nextGcPosition = binary.LittleEndian.Uint64(bz[start:end])
	start, end = end, end+8
	mi.activeByteCount = binary.LittleEndian.Uint64(bz[start:end])
	start, end = end, end+8
	mi.seed = binary.LittleEndian.Uint64(bz[start:end])
	start, end = end, end+8
	mi.blockSize = binary.LittleEndian.Uint64(bz[start:end])
	mi.closed = bz[end] != 0
}

type RabbitKV struct {
	mtx        sync.RWMutex
	hpfile     HPFile
	ilog       IndexLogger
	hb         Hash3Bundle
	wrLogCount uint64
	mi         MetaInfo
	metaFile   string
}

func (rkv *RabbitKV) Close() {
	rkv.mi.closed = true
	rkv.SaveMetaFile()
}

func (rkv *RabbitKV) SaveMetaFile() {
	f, err := os.OpenFile(rkv.metaFile, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	bz := rkv.mi.ToBytes()
	f.Write(bz[:])
	f.Close()
}

func LoadRabbitKV(hpfDirName, idxDirName, metaFile string) (res *RabbitKV, err error) {
	res = &RabbitKV{metaFile: metaFile}
	f, err := os.Open(metaFile)
	if err != nil {
		return
	}
	var buf [MetaInfoBytes]byte
	_, err = f.Read(buf[:])
	if err != nil {
		return
	}
	res.mi.FromBytes(buf)
	if !res.mi.closed {
		err = errors.New("RabbitKV is not closed properly")
		return
	}

	res.hpfile, err = NewHPFile(int(res.mi.blockSize), hpfDirName)
	if err != nil {
		return
	}
	res.ilog, err = NewIndexLogger(idxDirName)
	if err != nil {
		return
	}
	res.hb = NewHash3Bundle(res.mi.allAddrBits)
	res.ilog.Scan(func(key uint64, value uint32) {
		res.hb.Set(key, value)
	})

	if !res.mi.closed { // not closed properly
		res.RecoverMetaInfo()
	}
	res.mi.closed = false
	return
}

func (rkv *RabbitKV) GabageCollect(lengthLimit, countLimit int64) {
	start := int64(rkv.mi.nextGcPosition)
	rkv.ScanSlots(start, start + lengthLimit, func(slot Slot, pos uint64, _ int) bool {
		countLimit--
		if countLimit < 0 {
			return false // Stop Scan
		}
		key64 := meow.Checksum64(rkv.mi.seed, slot.pairs[0].key)
		if kv32 := rkv.hb.Find(key64); kv32 != nil {
			rkv.mtx.Lock()
			slices, _ := slot.ToSlicesForDump()
			pos, err := rkv.hpfile.Append(slices)
			if err != nil {
				panic(err)
			}
			if pos%16 != 0 {
				panic("Position is not X16")
			}
			kv32.Value = uint32(pos/16)
			rkv.WriteLog(key64, kv32.Value)
			rkv.mtx.Unlock()
		}
		return true
	})
}

func (rkv *RabbitKV) RecoverMetaInfo() {
	start, end := int64(rkv.mi.nextGcPosition), rkv.hpfile.Size()
	rkv.mi.nextGcPosition = math.MaxUint64
	rkv.mi.activeByteCount = 0
	rkv.ScanSlots(start, end, func(slot Slot, pos uint64, length int) bool {
		key64 := meow.Checksum64(rkv.mi.seed, slot.pairs[0].key)
		if rkv.hb.Find(key64) != nil {
			if rkv.mi.nextGcPosition == math.MaxUint64 {
				rkv.mi.nextGcPosition = pos
			}
			rkv.mi.activeByteCount += uint64(length)
		}
		return true
	})
}

func (rkv *RabbitKV) ScanSlots(start, end int64, fn func(slot Slot, pos uint64, length int) bool) {
	for pos, length := start, 0; pos < end; pos += int64(length) {
		slot, length := rkv.ReadSlot(pos)
		if !fn(slot, uint64(pos), length) {
			break
		}
	}
}

func (rkv *RabbitKV) ReadSlot(pos int64) (Slot, int) {
	var buf [4]byte
	err := rkv.hpfile.ReadAt(buf[:], pos)
	if err != nil {
		panic(err)
	}
	length := int(binary.LittleEndian.Uint32(buf[:]))
	bz := make([]byte, length)
	err = rkv.hpfile.ReadAt(bz, pos+4)
	if err != nil {
		panic(err)
	}
	slot, err := BytesToSlot(bz)
	if err != nil {
		panic(err)
	}
	return slot, length+4
}

func (rkv *RabbitKV) Get(key []byte) []byte {
	rkv.mtx.RLock()
	defer rkv.mtx.RUnlock()
	key64 := meow.Checksum64(rkv.mi.seed, key)
	kv32 := rkv.hb.Find(key64)
	if !kv32.IsValid() {
		return nil
	}
	pos := int64(kv32.Value)*16
	slot, _ := rkv.ReadSlot(pos)
	value := slot.Get(key)
	if value == nil {
		return nil
	} else {
		return append([]byte{}, value...)
	}
}

func (rkv *RabbitKV) Set(key, value []byte) {
	if value == nil {
		panic("Cannot set a nil value")
	}
	rkv.update(key, value)
}

func (rkv *RabbitKV) Delete(key []byte) {
	rkv.update(key, nil)
}

func (rkv *RabbitKV) update(key, value []byte) {
	rkv.mtx.Lock()
	defer rkv.mtx.Unlock()
	key64 := meow.Checksum64(rkv.mi.seed, key)
	kv32, status := rkv.hb.FindX(key64)
	if status == Found {
		pos := int64(kv32.Value)*16
		slot, length := rkv.ReadSlot(pos)
		rkv.mi.activeByteCount -= uint64(length)
		if value == nil { //deletion
			slot.Remove(key)
		} else {
			slot.Add(Pair{key, value})
		}
		if slot.Empty() {
			kv32.Value = 0 // invalidate it
		} else {
			slices, newLen := slot.ToSlicesForDump()
			rkv.mi.activeByteCount += uint64(newLen)
			pos, err := rkv.hpfile.Append(slices)
			if err != nil {
				panic(err)
			}
			if pos%16 != 0 {
				panic("Position is not X16")
			}
			kv32.Value = uint32(pos/16)
		}
		rkv.WriteLog(key64, kv32.Value)
		return
	}

	if value == nil { //nothing to do for deletion when NotFound
		return
	}

	if status == NotFoundAndFull {
		rkv.hb.EnlargeForKey(key64)
		kv32, status = rkv.hb.FindX(key64)
	}

	if status == Found || status == NotFoundAndFull {
		panic("Impossible case, bug here")
	}

	// now status == NotFoundAndCanInsert
	slot := NewSlot(key, value)
	slices, newLen := slot.ToSlicesForDump()
	pos, err := rkv.hpfile.Append(slices)
	rkv.mi.activeByteCount += uint64(newLen)
	if err != nil {
		panic(err)
	}
	if pos%16 != 0 {
		panic("Position is not X16")
	}
	kv32.Value = uint32(pos/16)
	rkv.WriteLog(key64, kv32.Value)
}

func (rkv *RabbitKV) WriteLog(key64 uint64, value32 uint32) {
	rkv.ilog.Write(key64, value32)
	rkv.wrLogCount++
	if rkv.wrLogCount%1024 == 0 {
		estBytesInHash3 := rkv.hb.EstimatedCount()*EntryLengthInLog/256
		if rkv.ilog.SizeOfLastFile() > 4*estBytesInHash3 {
			rkv.ilog.AddNewFile(&rkv.hb)
			rkv.SaveMetaFile() //just record allAddrBits
		}
	}
}

func (rkv *RabbitKV) Sync() {
	err := rkv.ilog.Sync()
	if err != nil {
		panic(err)
	}
	err = rkv.hpfile.Sync()
	if err != nil {
		panic(err)
	}
}

// ============================================

type Batch struct {
	rkv   *RabbitKV
	cache map[string][]byte
}

func (rkv *RabbitKV) NewBatch() *Batch {
	return &Batch{
		rkv:   rkv,
		cache: make(map[string][]byte),
	}
}

func (batch *Batch) Close() {
	for k, v := range batch.cache {
		batch.rkv.update([]byte(k), v)
	}
	batch.rkv.Sync()
	batch.cache = nil
}

func (batch *Batch) Get(key []byte) []byte {
	v, ok := batch.cache[string(key)]
	if ok {
		return v
	}
	return batch.rkv.Get(key)
}

func (batch *Batch) Set(key, value []byte) {
	if value == nil {
		panic("Cannot set a nil value")
	}
	batch.cache[string(key)] = value
}

func (batch *Batch) Delete(key []byte) {
	batch.cache[string(key)] = nil
}

