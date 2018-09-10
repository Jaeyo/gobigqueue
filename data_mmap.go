package gobigqueue

import (
	"fmt"
	"os"

	"github.com/go-errors/errors"
	"github.com/jaeyo/gobigqueue/utils"
)

/*
END_OF_FILE_OFFSET을 표시해놓기 위해 처음 4바이트는 비워놓고 시작한다.
END_OF_FILE_OFFSET
	* 0: 아직 끝까지 쓰지 않음
	* n: 마지막에 담긴 데이터의 position + 1
*/
type DataMmap struct {
	mmap *MmapWrapper
}

func (dataMmap *DataMmap) Enqueue(data []byte, pos int) {
	dataMmap.mmap.Set(utils.UintToBytes(len(data)), pos)
	dataMmap.mmap.Set(data, pos+4)
}

func (dataMmap *DataMmap) Dequeue(pos int) []byte {
	length := utils.BytesToUint(dataMmap.mmap.Get(pos, 4))
	return dataMmap.mmap.Get(pos+4, length)
}

func (dataMmap *DataMmap) SetEndOfFileOffset(offset int) {
	dataMmap.mmap.Set(utils.UintToBytes(offset), 0)
}

func (dataMmap *DataMmap) GetEndOfFileOffset() int {
	return utils.BytesToUint(dataMmap.mmap.Get(0, 4))
}

func (dataMmap *DataMmap) Close() error {
	return dataMmap.mmap.Close()
}

func NewDataMmap(filename string) (*DataMmap, error) {
	exists, err := utils.Exists("./data")
	if err != nil {
		return nil, err
	}
	if exists == false {
		err := os.Mkdir("./data", 0777)
		if err != nil {
			return nil, errors.Errorf(err.Error())
		}
	}

	mmap, _, err := NewMmap(fmt.Sprintf("./data/%s", filename), DATA_FILE_SIZE)
	if err != nil {
		return nil, err
	}

	dataMmap := DataMmap{mmap}
	dataMmap.SetEndOfFileOffset(0)

	return &dataMmap, nil
}
