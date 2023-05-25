package index

import (
	fmt "fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	BlockNumIdxKeyPrefix        = 'n'
	BlockHashIdxKeyPrefix       = 'h'
	TxIDIdxKeyPrefix            = 't'
	BlockNumTranNumIdxKeyPrefix = 'a'
	BlockfilePrefix             = "blockfile_"
	IndexSavePointKeyStr        = "indexCheckpointKey"
)

var (
	dbNameKeySep = []byte{0x00}
	//compositeKeySep = []byte{0x00}
	LastKeyIndicator = byte(0x01)
	//formatVersionKey  = []byte{'f'} // a single key in db whose value indicates the version of the data format
	IndexSavePointKey = []byte(IndexSavePointKeyStr)
	BlkMgrInfoKey     = []byte("blkMgrInfo")
)

type dataKey []byte

type FileLocPointer struct {
	FileSuffixNum int //blockfile_**文件后缀的number
	LocPointer
}

type LocPointer struct {
	Offset      int //blockfile_**文件内偏移量
	BytesLength int //当前信息字节长度
}

func (flp *FileLocPointer) Marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	e := buffer.EncodeVarint(uint64(flp.FileSuffixNum))
	if e != nil {
		return nil, errors.Wrapf(e, "unexpected error while marshaling fileLocPointer [%s]", flp)
	}
	e = buffer.EncodeVarint(uint64(flp.Offset))
	if e != nil {
		return nil, errors.Wrapf(e, "unexpected error while marshaling fileLocPointer [%s]", flp)
	}
	e = buffer.EncodeVarint(uint64(flp.BytesLength))
	if e != nil {
		return nil, errors.Wrapf(e, "unexpected error while marshaling fileLocPointer [%s]", flp)
	}
	return buffer.Bytes(), nil
}

func (flp *FileLocPointer) Unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	i, e := buffer.DecodeVarint()
	if e != nil {
		fmt.Println("DecodeVarint", e)
	}
	flp.FileSuffixNum = int(i)

	i, e = buffer.DecodeVarint()
	if e != nil {
		fmt.Println("DecodeVarint", e)
	}
	flp.Offset = int(i)
	i, e = buffer.DecodeVarint()
	if e != nil {
		fmt.Println("DecodeVarint", e)
	}
	flp.BytesLength = int(i)
	return nil
}

func EncodeBlockNum(blockNum uint64) []byte {
	return proto.EncodeVarint(blockNum)
}

func DecodeBlockNum(blockNumBytes []byte) uint64 {
	blockNum, _ := proto.DecodeVarint(blockNumBytes)
	return blockNum
}

type BlockfilesInfo struct {
	LatestFileNumber   int
	LatestFileSize     int
	NoBlockFiles       bool
	LastPersistedBlock uint64
}

func (i *BlockfilesInfo) Marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	var err error
	if err = buffer.EncodeVarint(uint64(i.LatestFileNumber)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileNumber [%d]", i.LatestFileNumber)
	}
	if err = buffer.EncodeVarint(uint64(i.LatestFileSize)); err != nil {
		return nil, errors.Wrapf(err, "error encoding the latestFileSize [%d]", i.LatestFileSize)
	}
	if err = buffer.EncodeVarint(i.LastPersistedBlock); err != nil {
		return nil, errors.Wrapf(err, "error encoding the lastPersistedBlock [%d]", i.LastPersistedBlock)
	}
	var noBlockFilesMarker uint64
	if i.NoBlockFiles {
		noBlockFilesMarker = 1
	}
	if err = buffer.EncodeVarint(noBlockFilesMarker); err != nil {
		return nil, errors.Wrapf(err, "error encoding noBlockFiles [%d]", noBlockFilesMarker)
	}
	return buffer.Bytes(), nil
}

func (i *BlockfilesInfo) Unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	var val uint64
	var noBlockFilesMarker uint64
	var err error

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.LatestFileNumber = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.LatestFileSize = int(val)

	if val, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.LastPersistedBlock = val
	if noBlockFilesMarker, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.NoBlockFiles = noBlockFilesMarker == 1
	return nil
}

func ConstructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func ConstructBlockNumKey(blockNum uint64) []byte {
	blkNumBytes := util.EncodeOrderPreservingVarUint64(blockNum)
	return append([]byte{BlockNumIdxKeyPrefix}, blkNumBytes...)
}

func UpdateLastBlockIndex(db *leveldb.DB, channel string, blockNum uint64) error {
	blkB := EncodeBlockNum(blockNum)
	blockNumKey := ConstructLevelKey(channel, IndexSavePointKey)
	err := db.Put(blockNumKey, blkB, &opt.WriteOptions{})
	if err != nil {
		fmt.Println("UpdateLastBlockIndex error")
	}
	return nil
}

func UpdateBlkMgrInfo(db *leveldb.DB, channel string, lpb uint64, lfn, lfs int, nbf bool) error {
	bfi := &BlockfilesInfo{}
	bfi.LastPersistedBlock = lpb
	bfi.LatestFileNumber = lfn
	bfi.LatestFileSize = lfs
	bfi.NoBlockFiles = nbf
	value, err := bfi.Marshal()
	if err != nil {
		return err
	}
	key := ConstructLevelKey(channel, BlkMgrInfoKey)
	err = db.Put(key, value, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}

func ConstructDataKey(ns string, key string, blocknum uint64, trannum uint64) dataKey {
	k := append([]byte(ns), dbNameKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(uint64(len(key)))...)
	k = append(k, []byte(key)...)
	k = append(k, dbNameKeySep...)
	k = append(k, util.EncodeOrderPreservingVarUint64(blocknum)...)
	k = append(k, util.EncodeOrderPreservingVarUint64(trannum)...)
	return dataKey(k)
}
