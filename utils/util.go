package utils

import (
	"os"
	"encoding/binary"
	"path/filepath"
	"github.com/go-errors/errors"
)

func Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, errors.Errorf(err.Error())
}

func UintToBytes(value int) []byte {
	bytesValue := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesValue, uint32(value))
	return bytesValue
}

func BytesToUint(bytesValue []byte) int {
	return int(binary.LittleEndian.Uint32(bytesValue))
}

func GetCurrentPath() string {
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	return dir
}