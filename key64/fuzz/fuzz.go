package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	//"sort"

	"github.com/coinexchain/randsrc"
	"github.com/wide-key/RabbitKV/key64"
)

const KeyLen = 16
const ValueLen = 9

type FuzzConfig struct {
	MaxSize         int
	MinSize         int
	RunSteps        int
	CompareInterval int
}

func FuzzSet(cfg *FuzzConfig, rs randsrc.RandSrc, refMap map[[KeyLen]byte][]byte, db *key64.DB) {
	var key [KeyLen]byte
	for i:=0; i<cfg.RunSteps; i++ {
		copy(key[:], rs.GetBytes(KeyLen))
		value := rs.GetBytes(ValueLen)
		refMap[key] = value
		db.Set(key[:], value)
	}
}

func FuzzModify(cfg *FuzzConfig, rs randsrc.RandSrc, refMap map[[KeyLen]byte][]byte, db *key64.DB) {
	finishedSteps := 0
	skippedEntries := 0
	toBeSkipped := int(rs.GetUint64()) % len(refMap) - cfg.RunSteps
	if toBeSkipped < 0 {
		toBeSkipped = 0
	}

	//keyList := make([][KeyLen]byte, 0, len(refMap))
	//for k := range refMap {
	//	keyList = append(keyList, k)
	//}
	//sort.Slice(keyList, func(i, j int) bool {
	//	return bytes.Compare(keyList[i][:], keyList[j][:]) < 0
	//})
	//for _, k := range keyList {
	//	v := refMap[k]

	for k, v := range refMap {
		if skippedEntries < toBeSkipped {
			skippedEntries++
			continue
		}
		dbV, ok := db.Get(k[:])
		if !ok || !bytes.Equal(dbV,v) {
			fmt.Printf("ok:%v k:%v dbV:%v v:%v\n", ok, k[:], dbV, v)
			fmt.Printf("The entry %#v\n", db.GetEntry([2]byte{157, 67}))
			fmt.Printf("The entry %v\n", db.GetEntry([2]byte{157, 67}))
			panic("Compare Error")
		}
		if rs.GetUint64() % 2 == 0 { //delete
			db.Delete(k[:])
			delete(refMap, k)
			_, ok = db.Get(k[:])
			if ok {
				fmt.Printf("The entry %#v\n", db.GetEntry([2]byte{206, 75}))
				fmt.Printf("The entry %v\n", db.GetEntry([2]byte{206, 75}))
				fmt.Printf("The deleted entry %v\n", k[:])
				panic("Why? it was deleted...")
			}
		} else { //change
			newV := rs.GetBytes(ValueLen)
			refMap[k] = newV
			db.Set(k[:], newV)
		}
		finishedSteps++
		if finishedSteps >= cfg.RunSteps {
			break
		}
	}
}

func RunCompare(refMap map[[KeyLen]byte][]byte, db *key64.DB) {
	for k, v := range refMap {
		dbV, ok := db.Get(k[:])
		if !ok || !bytes.Equal(dbV,v) {
			fmt.Printf("ok:%v dbV:%v v:%v\n", ok, dbV, v)
			panic("Compare Error")
		}
	}
}

func RunFuzz(cfg *FuzzConfig, roundCount int, randFilename string) {
	if key64.KeySize != 2 {
		panic("key64.KeySize != 2")
	}
	rs := randsrc.NewRandSrcFromFile(randFilename)
	refMap := make(map[[KeyLen]byte][]byte, cfg.MaxSize)
	db := key64.NewDB()
	for i := 0; i < roundCount; i++ {
		if i % 100 == 0 {
			fmt.Printf("now round %d\n", i)
		}
		if len(refMap) < cfg.MaxSize {
			//fmt.Printf("FuzzSet\n")
			FuzzSet(cfg, rs, refMap, db)
		}
		if len(refMap) > cfg.MinSize {
			//fmt.Printf("FuzzModify\n")
			FuzzModify(cfg, rs, refMap, db)
		}
		if i % cfg.CompareInterval == 0 {
			//fmt.Printf("RunCompare\n")
			RunCompare(refMap, db)
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s <rand-source-file> <round-count>\n", os.Args[0])
		return
	}
	randFilename := os.Args[1]
	roundCount, err := strconv.Atoi(os.Args[2])

	if err != nil {
		panic(err)
	}

	cfg := &FuzzConfig{
		MaxSize:         256*256/16,
		MinSize:         256*256/32,
		RunSteps:        100,
		CompareInterval: 1000,
	}

	RunFuzz(cfg, roundCount, randFilename)
}

