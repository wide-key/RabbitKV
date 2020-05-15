package key64

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	sha256 "github.com/minio/sha256-simd"
	"github.com/stretchr/testify/assert"
)

func conflictPath(key []byte, path [][KeySize]byte) bool {
	hash := sha256.Sum256(key)
	for _, k := range path {
		if !bytes.Equal(hash[:KeySize], k[:]) {
			return false
		}
		hash = sha256.Sum256(hash[:])
	}
	return true
}

func findConflict(start uint64, path [][KeySize]byte) uint64 {
	var buf [8]byte
	for {
		binary.LittleEndian.PutUint64(buf[:], start)
		if conflictPath(buf[:], path) {
			return start
		}
		start++
	}
}

func showPath(key []byte, depth int) {
	hash := sha256.Sum256(key)
	for i := 0; i < depth; i++ {
		fmt.Printf("%d %v\n", i, hash[:KeySize])
		hash = sha256.Sum256(hash[:])
	}
}

// 99         [229 250] [109 247] [246 73]
// 2289957235 [229 250] [109 247] [244 197]
// 5454187569 [229 250] [109 247] [114 126]

// 18983      [229 250] [249 185] [49 26]
// 1062734    [229 250] [196 212] [123 123]

// 88713 [109 247] [162 182] [55 209]

func tryGet(t *testing.T, db *DB, key []byte, value string) {
	v, ok := db.Get(key)
	assert.Equal(t, true, ok)
	assert.Equal(t, value, string(v))
}

func Test2(t *testing.T) {
	showPath(TheKey, 4)
}

func Test1(t *testing.T) {
	if KeySize != 2 {
		fmt.Printf("UnitTest Not Run Because KeySize != 2\n")
		return
	}
	keyABL := make([]byte, 8)
	keyABM := make([]byte, 8)
	keyABN := make([]byte, 8)
	keyAPX := make([]byte, 8)
	keyAQY := make([]byte, 8)
	keyBTZ := make([]byte, 8)
	binary.LittleEndian.PutUint64(keyABL, 99)
	binary.LittleEndian.PutUint64(keyABM, 2289957235)
	binary.LittleEndian.PutUint64(keyABN, 5454187569)
	binary.LittleEndian.PutUint64(keyAPX, 18983)
	binary.LittleEndian.PutUint64(keyAQY, 1062734)
	binary.LittleEndian.PutUint64(keyBTZ, 88713)

	//showPath(keyABL, 3)
	//showPath(keyABM, 3)
	//showPath(keyABN, 3)
	//showPath(keyAPX, 3)
	//showPath(keyAQY, 3)
	//showPath(keyBTZ, 3)

	db := NewDB()
	db.Set(keyBTZ, []byte("keyBTZ"))
	db.Set(keyAPX, []byte("keyAPX"))
	_, ok := db.Get(keyABM)
	assert.Equal(t, false, ok)

	db.Set(keyAQY, []byte("keyAQY"))
	db.Set(keyABL, []byte("keyABL"))
	db.Set(keyABM, []byte("keyABM"))
	db.Set(keyABN, []byte("keyABN"))

	tryGet(t, db, keyBTZ, "keyBTZ")
	tryGet(t, db, keyAPX, "keyAPX")
	tryGet(t, db, keyAQY, "keyAQY")
	tryGet(t, db, keyABL, "keyABL")
	tryGet(t, db, keyABM, "keyABM")
	tryGet(t, db, keyABN, "keyABN")

	_, ok = db.Get([]byte{0,0,0})
	assert.Equal(t, false, ok)

	db.Set(keyABL, []byte("keyabl"))
	tryGet(t, db, keyABL, "keyabl")
	db.Delete([]byte{0,0,0})
	db.Delete(keyBTZ)
	db.Set(keyBTZ, []byte("keybtz"))
	tryGet(t, db, keyBTZ, "keybtz")

	db.Delete(keyABN)
	_, ok = db.Get(keyABN)
	assert.Equal(t, false, ok)
	db.Delete(keyABL)
	_, ok = db.Get(keyABL)
	assert.Equal(t, false, ok)
	db.Delete(keyABM)
	_, ok = db.Get(keyABM)
	assert.Equal(t, false, ok)

	tryGet(t, db, keyAPX, "keyAPX")
	tryGet(t, db, keyAQY, "keyAQY")
}

//func Test0(t *testing.T) {
//	var buf [8]byte
//	binary.LittleEndian.PutUint64(buf[:], 99)
//	showPath(buf[:], 3)
//
//	path := make([][2]byte, 1)
//	//path[0] = [2]byte{229, 250}
//	//path[1] = [2]byte{109, 247}
//	path[0] = [2]byte{109, 247}
//	i := findConflict(1, path)
//	fmt.Printf("Here!!! %d\n", i)
//	binary.LittleEndian.PutUint64(buf[:], i)
//	showPath(buf[:], 3)
//}

