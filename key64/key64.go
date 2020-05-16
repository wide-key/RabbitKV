package key64

import (
	"bytes"

	sha256 "github.com/minio/sha256-simd"
)

const (
	KeySize = 2 // 8

	NotFount = 0
	EmptySlot = 1
	Exists = 2

	MaxFindDepth = 100
)

type ValueX struct {
	key       []byte
	value     []byte
	passByNum int64
	deleted   bool
}

type DB struct {
	m  map[[KeySize]byte]*ValueX
}

func NewDB() *DB {
	return &DB{m: make(map[[KeySize]byte]*ValueX)}
}

func (db *DB) GetEntry(k [KeySize]byte) *ValueX {
	return db.m[k]
}

func (db *DB) NumUsedEntries() int {
	return len(db.m)
}

var TheKey = []byte{224, 199, 23, 63, 67, 252, 54, 110, 198, 250, 189, 26, 18, 160, 19, 230}

func (db *DB) Get(key []byte) (value []byte, ok bool) {
	value, path := db.find(key, true)
	ok = path.status == Exists
	return
}

type Path struct {
	keyList       [][KeySize]byte
	status        int
	firstEmptyPos int
}

func (db *DB) find(key []byte, earlyExit bool) (value []byte, path Path) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	path.status = NotFount
	path.firstEmptyPos = -1
	for i := 0; i < MaxFindDepth; i++ {
		copy(k[:], hash[:])
		path.keyList = append(path.keyList, k)
		vx, foundIt := db.m[k]
		if !foundIt {
			return
		}
		if vx.deleted && path.firstEmptyPos < 0 {
			path.firstEmptyPos = i
		}
		if bytes.Equal(vx.key, key) {
			value = vx.value
			path.status = Exists
			if vx.deleted {
				path.status = EmptySlot
			}
			return
		} else if earlyExit && vx.passByNum == 0 {
			return
		} else {
			hash = sha256.Sum256(hash[:])
		}
	}
	panic("MaxFindDepth reached!")
}

func isWatched(k [KeySize]byte) bool {
	if bytes.Equal(k[:], []byte{130,123}) {return true}
	if bytes.Equal(k[:], []byte{91,  36}) {return true}
	if bytes.Equal(k[:], []byte{110,255}) {return true}
	if bytes.Equal(k[:], []byte{0,  255}) {return true}
	return false
}

func (db *DB) Set(key []byte, value []byte) {
	_, path := db.find(key, false)
	kl := path.keyList
	if path.status == Exists { //change
		db.m[kl[len(kl)-1]].value = append([]byte{}, value...)
		return
	}
	if path.status == EmptySlot { //overwrite
		db.m[kl[len(kl)-1]].key = append([]byte{}, key...)
		db.m[kl[len(kl)-1]].value = append([]byte{}, value...)
		db.m[kl[len(kl)-1]].deleted = false
	} else { //insert
		db.m[kl[len(kl)-1]] = &ValueX{
			key:       append([]byte{}, key...),
			value:     append([]byte{}, value...),
			passByNum: 0,
			deleted:   false,
		}
	}
	// incr passByNum
	for _, k := range kl[:len(kl)-1] {
		db.m[k].passByNum++
	}
}

func (db *DB) Delete(key []byte) {
	_, path := db.find(key, true)
	kl := path.keyList
	if path.status != Exists {
		return
	}
	if db.m[kl[len(kl)-1]].passByNum == 0 { // can delete it
		delete(db.m, kl[len(kl)-1])
	} else { // can not delete it, just mark it as deleted
		db.m[kl[len(kl)-1]].deleted = true
	}
	for _, k := range kl[:len(kl)-1] {
		db.m[k].passByNum--
		if db.m[k].passByNum == 0 && db.m[k].deleted {
			delete(db.m, k)
		}
	}
}

