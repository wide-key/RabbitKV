package key64

import (
	"bytes"
	"crypto/sha256"
)

const (
	KeySize = 8
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

func (db *DB) Get(key []byte) (value []byte, ok bool) {
	value, _, ok = db.get(key)
	return
}

func (db *DB) get(key []byte) (value []byte, path [][KeySize]byte, ok bool) {
	var k [KeySize]byte
	hash := sha256.Sum256(key)
	for {
		copy(k[:], hash[:])
		path = append(path, k)
		v, hasIt := db.m[k]
		if hasIt {
			if bytes.Equal(v.key, key) && !v.deleted {
				value = v.value
				ok = true
				return
			} else if v.passByNum == 0 {
				return
			} else {
				hash = sha256.Sum256(hash[:])
			}
		} else {
			return
		}
	}
}

func (db *DB) Set(key []byte, value []byte) {
	_, path, ok := db.get(key)
	if ok { //change
		db.m[path[len(path)-1]].value = append([]byte{}, value...)
		return
	}
	//insert
	db.m[path[len(path)-1]] = &ValueX{
		key:       append([]byte{}, key...),
		value:     append([]byte{}, value...),
		passByNum: 0,
		deleted:   false,
	}
	// incr passByNum
	for _, k := range path[:len(path)-1] {
		db.m[k].passByNum++
	}
}

func (db *DB) Delete(key []byte) {
	_, path, ok := db.get(key)
	if !ok {
		return
	}
	if db.m[path[len(path)-1]].passByNum == 0 {
		delete(db.m, path[len(path)-1])
	}
	for _, k := range path[:len(path)-1] {
		db.m[k].passByNum--
		if db.m[k].passByNum == 0 && !db.m[k].deleted {
			delete(db.m, k)
		}
	}
}


