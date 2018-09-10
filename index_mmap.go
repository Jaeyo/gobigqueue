package gobigqueue

import (
	"path/filepath"

	"github.com/jaeyo/gobigqueue/utils"
)

/*
4바이트씩 끊어서
	0. data file 갯수: 다음번 data file을 만들때 참고
	1. 현재 head가 잡고있는 데이터 파일 번호
	2. 현재 head가 잡고있는 데이터 파일 내에서의 position
	3. 현재 tail이 잡고있는 데이터 파일 번호
	4. 현재 tail가 잡고있는 데이터 파일 내에서의 position
	TODO IMME
*/

type IndexMmap struct {
	mmap *MmapWrapper
}

func (indexMmap *IndexMmap) SetDataCountNumMax(index int) {
	indexMmap.mmap.Set(utils.UintToBytes(index), 0*4)
}

func (indexMmap *IndexMmap) GetDataCountNumMax() int {
	return utils.BytesToUint(indexMmap.mmap.Get(0*4, 4))
}

func (indexMmap *IndexMmap) SetHeadDataFileIndex(index int) {
	indexMmap.mmap.Set(utils.UintToBytes(index), 1*4)
}

func (indexMmap *IndexMmap) GetHeadDataFileIndex() int {
	return utils.BytesToUint(indexMmap.mmap.Get(1*4, 4))
}

func (indexMmap *IndexMmap) SetHeadPos(pos int) {
	indexMmap.mmap.Set(utils.UintToBytes(pos), 2*4)
}

func (indexMmap *IndexMmap) GetHeadPos() int {
	return utils.BytesToUint(indexMmap.mmap.Get(2*4, 4))
}

func (indexMmap *IndexMmap) SetTailDataFileIndex(index int) {
	indexMmap.mmap.Set(utils.UintToBytes(index), 3*4)
}

func (indexMmap *IndexMmap) GetTailDataFileIndex() int {
	return utils.BytesToUint(indexMmap.mmap.Get(3*4, 4))
}

func (indexMmap *IndexMmap) SetTailPos(pos int) {
	indexMmap.mmap.Set(utils.UintToBytes(pos), 4*4)
}

func (indexMmap *IndexMmap) GetTailPos() int {
	return utils.BytesToUint(indexMmap.mmap.Get(4*4, 4))
}

func (indexMmap *IndexMmap) Close() error {
	return indexMmap.mmap.Close()
}

func NewIndexMmap() (*IndexMmap, error) {
	dataPath, err := GetDataPath()
	if err != nil {
		return nil, err
	}

	indexDataPath := filepath.Join(dataPath, "index.dat")

	mmap, isNew, err := NewMmap(indexDataPath, INDEX_FILE_LENGTH*4)
	if err != nil {
		return nil, err
	}

	indexMmap := &IndexMmap{mmap}

	if isNew == true {
		indexMmap.SetDataCountNumMax(0)
		indexMmap.SetHeadDataFileIndex(0)
		indexMmap.SetHeadPos(4)
		indexMmap.SetTailDataFileIndex(0)
		indexMmap.SetTailPos(4)
	}

	return indexMmap, nil
}
