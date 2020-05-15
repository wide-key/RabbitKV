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
	value, _, status := db.findPathForDelete(key)
	ok = status == Exists
	return
}

func (db *DB) findPathForSet(key []byte) (path [][KeySize]byte, status int) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	for i := 0; i < MaxFindDepth; i++ {
		copy(k[:], hash[:])
		path = append(path, k)
		vx, foundIt := db.m[k]
		if !foundIt {
			return
		}
		if vx.deleted {
			status = EmptySlot
			return
		} else if bytes.Equal(vx.key, key) {
			status = Exists
			return
		} else {
			hash = sha256.Sum256(hash[:])
		}
	}
	panic("MaxFindDepth reached!")
}

func (db *DB) findPathForDelete(key []byte) (value []byte, path [][KeySize]byte, status int) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	for i := 0; i < MaxFindDepth; i++ {
		copy(k[:], hash[:])
		path = append(path, k)
		vx, foundIt := db.m[k]
		if !foundIt {
			return
		}
		if bytes.Equal(vx.key, key) {
			value = vx.value
			status = Exists
			if vx.deleted {
				status = EmptySlot
			}
			return
		} else if vx.passByNum == 0 {
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
	path, status := db.findPathForSet(key)
	if status == Exists { //change
		db.m[path[len(path)-1]].value = append([]byte{}, value...)
		return
	} else if status == EmptySlot { //overwrite
		db.m[path[len(path)-1]].key = append([]byte{}, key...)
		db.m[path[len(path)-1]].value = append([]byte{}, value...)
		db.m[path[len(path)-1]].deleted = false
	} else { //insert
		db.m[path[len(path)-1]] = &ValueX{
			key:       append([]byte{}, key...),
			value:     append([]byte{}, value...),
			passByNum: 0,
			deleted:   false,
		}
	}
	// incr passByNum
	for _, k := range path[:len(path)-1] {
		db.m[k].passByNum++
	}
}

func (db *DB) Delete(key []byte) {
	_, path, status := db.findPathForDelete(key)
	if bytes.Equal(key, TheKey) {
	}
	if status != Exists {
		return
	}
	if db.m[path[len(path)-1]].passByNum == 0 { // can delete it
		delete(db.m, path[len(path)-1])
	} else { // can not delete it, just mark it as deleted
		db.m[path[len(path)-1]].deleted = true
	}
	for _, k := range path[:len(path)-1] {
		db.m[k].passByNum--
		if db.m[k].passByNum == 0 && db.m[k].deleted {
			delete(db.m, k)
		}
	}
}


