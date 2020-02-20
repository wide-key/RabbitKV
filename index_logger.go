package rabbitkv

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"io/ioutil"
	"strconv"
	"sort"
)

const (
	EffectiveFileCount = 257
	EntryLengthInLog = 10
)

type IndexLogger struct {
	fileIDList []int64
	dirName    string
	outFile    *os.File
}

func getFileSize(f *os.File) int64 {
	size, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		panic(err)
	}
	return size
}

func (ilog *IndexLogger) SizeOfLastFile() int64 {
	return getFileSize(ilog.outFile)
}

func (ilog *IndexLogger) Write(key64 uint64, value32 uint32) (n int, err error) {
	var buf [3+EntryLengthInLog]byte
	binary.BigEndian.PutUint64(buf[0:8], key64)
	binary.BigEndian.PutUint32(buf[8:12], value32)
	for i:=3; i<12; i++ { // the first three bytes are ingored
		buf[12] ^= buf[i]
	}
	return ilog.outFile.Write(buf[3:])
}

func (ilog *IndexLogger) Sync() error {
	return ilog.Sync()
}

func (ilog *IndexLogger) AddNewFile(hb *Hash3Bundle) (err error) {
	if ilog.outFile != nil {
		ilog.outFile.Close()
	}
	largestID := ilog.fileIDList[len(ilog.fileIDList)-1]
	fname := filepath.Join(ilog.dirName, fmt.Sprintf("%d", largestID))
	ilog.outFile, err = os.OpenFile(fname, os.O_RDWR, 0700)
	if err != nil {
		return
	}
	ilog.fileIDList = append(ilog.fileIDList, largestID)
	// Dump a Hash3 at the beginning
	arrIdx := largestID%256
	hb.arr[arrIdx].Scan(func(key uint32, value uint32) {
		key64 := (uint64(arrIdx)<<32)|uint64(key)
		ilog.Write(key64, value)
	})
	//remove old and useless log files
	for _, fileID := range ilog.fileIDList {
		if fileID > largestID - EffectiveFileCount {
			break
		}
		fname = filepath.Join(ilog.dirName, fmt.Sprintf("%d", fileID))
		err = os.Remove(fname)
		if err != nil {
			return
		}
	}
	return
}

func scanLogsInFile(f *os.File, fn func(key uint64, value uint32)) {
	var buf [3+EntryLengthInLog]byte
	size := getFileSize(f)
	for off := int64(0); off < size; off += EntryLengthInLog {
		_, err := f.ReadAt(buf[3:], off)
		if err != nil {
			panic(err)
		}
		cksum := byte(0)
		for i:=3; i<12; i++ { // the first three bytes are ingored
			cksum ^= buf[i]
		}
		if cksum != buf[12] {
			panic("Checksum Error")
		}
		key64 := binary.BigEndian.Uint64(buf[0:8])
		value32 := binary.BigEndian.Uint32(buf[8:12])
		fn(key64, value32)
	}
}

func (ilog *IndexLogger) Scan(fn func(key uint64, value uint32)) {
	for _, id := range ilog.fileIDList {
		fname := filepath.Join(ilog.dirName, fmt.Sprintf("%d", id))
		f, err := os.Open(fname)
		if err != nil {
			panic(err)
		}
		scanLogsInFile(f, fn)
		f.Close()
	}
}

func NewIndexLogger(dirName string) (IndexLogger, error) {
	res := IndexLogger{
		fileIDList: make([]int64, EffectiveFileCount),
		dirName:    dirName,
	}
	fileInfoList, err := ioutil.ReadDir(dirName)
	if err != nil {
		return res, err
	}
	for _, fileInfo := range fileInfoList {
		if fileInfo.IsDir() {
			continue
		}
		id, err := strconv.ParseInt(fileInfo.Name(), 10, 63)
		if err != nil {
			return res, err
		}
		res.fileIDList = append(res.fileIDList, id)
	}
	sort.Slice(res.fileIDList, func(i, j int) bool {
		return res.fileIDList[i] < res.fileIDList[j]
	})
	return res, nil
}

