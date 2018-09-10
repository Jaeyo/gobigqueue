package gobigqueue

import (
	"os"
	"sync"
	"syscall"

	"github.com/go-errors/errors"
	"github.com/jaeyo/gobigqueue/utils"
)

type MmapWrapper struct {
	mapFile   *os.File
	mmap      []byte
	writeLock *sync.Mutex
}

func (mmap *MmapWrapper) Set(data []byte, pos int) {
	mmap.writeLock.Lock()
	defer mmap.writeLock.Unlock()

	endPos := pos + len(data)
	copy(mmap.mmap[pos:endPos], data[:])
}

func (mmap *MmapWrapper) Get(pos, length int) []byte {
	endPos := pos + length

	data := make([]byte, length)

	copy(data[:], mmap.mmap[pos:endPos])
	return data
}

func (mmap *MmapWrapper) Close() error {
	err := syscall.Munmap(mmap.mmap)
	if err != nil {
		return errors.Errorf(err.Error())
	}

	err = mmap.mapFile.Close()
	if err != nil {
		return errors.Errorf(err.Error())
	}

	return nil
}

func newMapFile(filename string, length int64) (*os.File, error) {
	mapFile, err := os.Create(filename)
	if err != nil {
		return nil, errors.Errorf(err.Error())
	}

	_, err = mapFile.Seek(length-1, 0)
	if err != nil {
		return nil, errors.Errorf(err.Error())
	}

	_, err = mapFile.Write([]byte(" "))
	if err != nil {
		return nil, errors.Errorf(err.Error())
	}

	return mapFile, nil
}

func mmap(mapFile *os.File, length int) ([]byte, error) {
	mmap, err := syscall.Mmap(int(mapFile.Fd()), 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, errors.Errorf(err.Error())
	}

	return mmap, nil
}

func NewMmap(filename string, length int) (*MmapWrapper, bool, error) {
	mapFile, isNew, err := func() (*os.File, bool, error) {
		if exists, _ := utils.Exists(filename); exists == false {
			mapFile, err := newMapFile(filename, int64(length))
			if err != nil {
				return nil, false, err
			}
			return mapFile, true, nil
		} else {
			mapFile, err := os.Open(filename)
			if err != nil {
				return nil, true, errors.Errorf(err.Error())
			}
			return mapFile, false, nil
		}
	}()
	if err != nil {
		return nil, isNew, err
	}

	mmap, err := mmap(mapFile, length)
	if err != nil {
		return nil, isNew, err
	}

	writeLock := &sync.Mutex{}

	return &MmapWrapper{mapFile, mmap, writeLock}, isNew, nil
}
