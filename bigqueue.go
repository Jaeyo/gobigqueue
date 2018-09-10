package gobigqueue

/*
# index file contains
	* head pos
		* data file pos (int)
		* pos (int)
	* tail pos
		* data file pos (int)
		* pos (int)
# data file contains
	* length of data (int)
	* data ([]byte)
*/

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-errors/errors"
	"github.com/jaeyo/gobigqueue/utils"
)

const (
	INDEX_FILE_LENGTH = 5
	DATA_FILE_SIZE    = 128 * 1024 * 1024
)

type BigQueue struct {
	index *IndexMmap
	datas map[int]*DataMmap
}

func (q *BigQueue) Close() error {
	err := q.index.Close()
	if err != nil {
		return err
	}

	for _, dataMmap := range q.datas {
		err := dataMmap.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (q *BigQueue) Enqueue(data []byte) error {
	if len(data) >= DATA_FILE_SIZE {
		return fmt.Errorf("data size is too big")
	}

	index := q.index.GetHeadDataFileIndex()
	pos := q.index.GetHeadPos()

	if pos+len(data) >= DATA_FILE_SIZE {
		moveHeadToNextDataFile(q.index, q.datas)

		index = q.index.GetHeadDataFileIndex()
		pos = q.index.GetHeadPos()
	}

	q.datas[index].Enqueue(data, pos)
	q.index.SetHeadPos(pos + 4 + len(data))

	return nil
}

func (q *BigQueue) Dequeue() ([]byte, error) {
	if q.IsQueueEmpty() == true {
		return nil, errors.Errorf("queue is empty")
	}

	index := q.index.GetTailDataFileIndex()
	pos := q.index.GetTailPos()
	EOFoffset := q.datas[index].GetEndOfFileOffset()

	if EOFoffset != 0 && pos == EOFoffset {
		index = index + 1
		pos = 4

		q.index.SetTailDataFileIndex(index)
		q.index.SetTailPos(pos)
	}

	data := q.datas[index].Dequeue(pos)
	q.index.SetTailPos(pos + 4 + len(data))
	return data, nil
}

func (q *BigQueue) IsQueueEmpty() bool {
	headDataIndex := q.index.GetHeadDataFileIndex()
	tailDataIndex := q.index.GetTailDataFileIndex()
	if headDataIndex == tailDataIndex {
		headPos := q.index.GetHeadPos()
		tailPos := q.index.GetTailPos()
		if headPos == tailPos {
			return true
		}
	}
	return false
}

func (q *BigQueue) Compact() error {
	tailIndex := q.index.GetTailDataFileIndex()

	files, err := getDataPathFiles()
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "data") && strings.HasSuffix(file.Name(), ".dat") {
			dataIndex, err := extractDataIndexFromFileName(file.Name())
			if err != nil {
				return err
			}

			if dataIndex < tailIndex {
				dataPath, err := GetDataPath()
				if err != nil {
					return err
				}
				os.Remove(filepath.Join(dataPath, file.Name()))
			}
		}
	}
	return nil
}

func NewBigQueueQueue() (*BigQueue, error) {
	index, err := NewIndexMmap()
	if err != nil {
		return nil, err
	}

	datas := make(map[int]*DataMmap)

	files, err := getDataPathFiles()
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "data") && strings.HasSuffix(file.Name(), ".dat") {
			dataIndex, err := extractDataIndexFromFileName(file.Name())
			if err != nil {
				return nil, err
			}
			datas[dataIndex], err = NewDataMmap(file.Name())
			if err != nil {
				return nil, err
			}
		}
	}

	if len(datas) == 0 {
		datas[0], err = NewDataMmap("data0.dat")
		if err != nil {
			return nil, err
		}
	}

	return &BigQueue{index, datas}, nil
}

func moveHeadToNextDataFile(indexMmap *IndexMmap, dataMmaps map[int]*DataMmap) error {
	index := indexMmap.GetHeadDataFileIndex()
	pos := indexMmap.GetHeadPos()

	dataMmaps[index].SetEndOfFileOffset(pos)

	indexMmap.SetHeadDataFileIndex(index + 1)
	indexMmap.SetHeadPos(4)

	newDataMmap, err := NewDataMmap(fmt.Sprintf("data%d.dat", index+1))
	if err != nil {
		return err
	}

	dataMmaps[index+1] = newDataMmap

	return nil
}

func extractDataIndexFromFileName(filename string) (int, error) {
	filename = strings.Replace(filename, "data", "", 1)
	filename = strings.Replace(filename, ".dat", "", 1)
	dataIndex, err := strconv.Atoi(filename)
	if err != nil {
		return -1, errors.Errorf(err.Error())
	}
	return dataIndex, nil
}

func GetDataPath() (string, error) {
	dataPath := filepath.Join(utils.GetCurrentPath(), "data")
	if exists, _ := utils.Exists(dataPath); exists == false {
		err := os.Mkdir(dataPath, 0777)
		if err != nil {
			return "", errors.Errorf(err.Error())
		}
	}
	return dataPath, nil
}

func getDataPathFiles() ([]os.FileInfo, error) {
	dataDir, err := GetDataPath()
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return nil, errors.Errorf(err.Error())
	}

	return files, nil
}
