package confighistory

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	keyPrefix                 = "s"
	separatorByte             = byte(0)
	nsStopper                 = byte(1)
	CollectionConfigNamespace = "lscc"
)

type compositeKey struct {
	NS, Key  string
	BlockNum uint64
}

func EncodeCompositeKey(ns, key string, blockNum uint64) []byte {
	b := []byte(keyPrefix + ns)
	b = append(b, separatorByte)
	b = append(b, []byte(key)...)
	return append(b, encodeBlockNum(blockNum)...)
}

func encodeBlockNum(blockNum uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.MaxUint64-blockNum)
	return b
}

func DecodeCompositeKey(b []byte) *compositeKey {
	blockNumStartIndex := len(b) - 8
	nsKeyBytes, blockNumBytes := b[1:blockNumStartIndex], b[blockNumStartIndex:]
	separatorIndex := bytes.Index(nsKeyBytes, []byte{separatorByte})
	ns, key := nsKeyBytes[0:separatorIndex], nsKeyBytes[separatorIndex+1:]
	return &compositeKey{string(ns), string(key), decodeBlockNum(blockNumBytes)}
}

func decodeBlockNum(blockNumBytes []byte) uint64 {
	return math.MaxUint64 - binary.BigEndian.Uint64(blockNumBytes)
}

func ConstructCollectionConfigKey(chaincodeName string) string {
	return chaincodeName + "~collection" // collection config key as in version 1.2 and we continue to use this in order to be compatible with existing data
}
