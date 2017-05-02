package kvraft

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
)

// itob returns an 8-byte big endian representation of v.
func Itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func Btoi(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func SplitString(str, flag string) []string {
	strs := strings.Split(str, flag)
	for i := 0; i < len(strs); i++ {
		strs[i] = strings.TrimSpace(strs[i])
		fmt.Println("peer:" + strs[i])
	}
	return strs
}

func hash(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func VerifyDir(targetDir, defaultDir string) (string, error) {
	var dir string
	if targetDir == "" {
		dir = defaultDir
	} else {
		dir = targetDir
	}
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return "", err
	}
	return dir, nil
}
