package index

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testLevelDB/config"
	"testLevelDB/index"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	goleveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

// const (
// 	blockNumIdxKeyPrefix        = 'n'
// 	blockHashIdxKeyPrefix       = 'h'
// 	txIDIdxKeyPrefix            = 't'
// 	blockNumTranNumIdxKeyPrefix = 'a'
// 	blockfilePrefix             = "blockfile_"
// 	indexSavePointKeyStr        = "indexCheckpointKey"
// )

// var (
// 	dbNameKeySep      = []byte{0x00}
// 	lastKeyIndicator  = byte(0x01)
// 	formatVersionKey  = []byte{'f'} // a single key in db whose value indicates the version of the data format
// 	indexSavePointKey = []byte(indexSavePointKeyStr)
// 	blkMgrInfoKey     = []byte("blkMgrInfo")
// )

// func constructLevelKey(dbName string, key []byte) []byte {
// 	return append(append([]byte(dbName), dbNameKeySep...), key...)
// }

func GetLastBlockIndexed(db *leveldb.DB, channel string, key []byte) (uint64, error) {
	indexSavePointKeyStr := "indexCheckpointKey"
	indexSavePointKey := []byte(indexSavePointKeyStr)
	blockIndex := index.ConstructLevelKey(channel, indexSavePointKey)
	value, err := db.Get(blockIndex, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	blockNum, _ := proto.DecodeVarint(value)
	fmt.Println("blockNum", blockNum)
	return blockNum, nil
}

// all
func ReadPeerAllIndex(db *leveldb.DB, channel string) {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		var a bytes.Buffer
		a.WriteString(channel)
		a.WriteByte(0)
		a.WriteRune('a')
		prefix_a := a.String()
		if strings.HasPrefix(key, prefix_a) {
			keyss := iter.Key()
			if len(keyss[8:]) == 0 {
				blockNum6, _ := proto.DecodeVarint(keyss[6:7])
				blockNum7, _ := proto.DecodeVarint(keyss[7:8])
				blkLoc := &index.FileLocPointer{}
				blkLoc.Unmarshal(iter.Value())
				v, err := json.Marshal(blkLoc)
				if err != nil {
					fmt.Println("json marshal err", err)
				}
				fmt.Printf("Key[hxyz a%d%d]=Value[%s]\n", blockNum6, blockNum7, string(v))
			} else {
				//blockNum6, _ := proto.DecodeVarint(keyss[6:7])
				blockNum7, _ := proto.DecodeVarint(keyss[7:8]) //blockNum这个字节代表区块高度
				blockNum8, _ := proto.DecodeVarint(keyss[8:])  //这个字节代表交易在区块顺序
				// blockNum6, _, _ := util.DecodeOrderPreservingVarUint64(keyss[6:])
				// blockNum7, _, _ := util.DecodeOrderPreservingVarUint64(keyss[7:8])
				// blockNum8, _, _ := util.DecodeOrderPreservingVarUint64(keyss[8:])
				//fmt.Println(blockNum6, blockNum7, blockNum8)
				//fmt.Println("bb===\n", blockNum6, blockNum7, blockNum8)
				blkLoc := &index.FileLocPointer{}
				blkLoc.Unmarshal(iter.Value())
				v, err := json.Marshal(blkLoc)
				if err != nil {
					fmt.Println("json marshal err", err)
				}
				fmt.Printf("Key[hxyz a%d%d]=Value[%s]\n", blockNum7, blockNum8, string(v))
			}
		}
		var n bytes.Buffer
		n.WriteString(channel)
		n.WriteByte(0)
		n.WriteRune('n')
		prefix_n := n.String()
		if strings.HasPrefix(key, prefix_n) {
			keyss := iter.Key()
			if len(keyss[7:]) == 0 {
				blockNum6, _ := proto.DecodeVarint(keyss[6:])
				blkLoc := &index.FileLocPointer{}
				blkLoc.Unmarshal(iter.Value())
				v, err := json.Marshal(blkLoc)
				if err != nil {
					fmt.Println("json marshal err", err)
				}
				fmt.Printf("Key[hxyz n%d]=Value[%s]\n", blockNum6, string(v))
			} else {
				//blockNum6, _ := proto.DecodeVarint(keyss[6:7])
				blockNum7, _ := proto.DecodeVarint(keyss[7:8])
				blkLoc := &index.FileLocPointer{}
				blkLoc.Unmarshal(iter.Value())
				v, err := json.Marshal(blkLoc)
				if err != nil {
					fmt.Println("json marshal err", err)
				}
				fmt.Printf("Key[hxyz n%d]=Value[%s]\n", blockNum7, string(v))
			}

		}
		var h bytes.Buffer
		h.WriteString(channel)
		h.WriteByte(0)
		h.WriteRune('h')
		prefix_h := h.String()
		if strings.HasPrefix(key, prefix_h) {
			keyss := iter.Key()
			keyHash := keyss[6:]

			hash := hex.EncodeToString(keyHash)
			//testBy, _ := hex.DecodeString(hash)
			//fmt.Println("blockHash===", string(testBy))
			blkLoc := &index.FileLocPointer{}
			blkLoc.Unmarshal(iter.Value())
			v, err := json.Marshal(blkLoc)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("Key[hxyz h%s]=Value[%s]\n", hash, string(v))
		}
		var t bytes.Buffer
		t.WriteString(channel)
		t.WriteByte(0)
		t.WriteRune('t')
		prefix_t := t.String()
		if strings.HasPrefix(key, prefix_t) {
			keyss := iter.Key()
			//keyss[6:8] 其中第一个字节代表txid长度占多少字节 第二个字节代表txid长度
			//blockNum, _ := proto.DecodeVarint(keyss[7:8])
			//blockNum, tt, _ := util.DecodeOrderPreservingVarUint64(keyss[6:8])
			//fmt.Println("blockNum", blockNum, tt)
			// fmt.Println(len(keyss))
			txidb := keyss[8:72]
			//fmt.Println("txid=", string(txidb))
			//blockNum1, _ := proto.DecodeVarint(keyss[72:73])
			blockNum2, _ := proto.DecodeVarint(keyss[73:74])
			blockNum3, _ := proto.DecodeVarint(keyss[74:75])
			//fmt.Println(blockNum2, blockNum3)
			//
			txIDIndex := &index.TxIDIndexValue{}
			proto.Unmarshal(iter.Value(), txIDIndex)
			//TxValidationCode

			fmt.Printf("txid=%s  TxValidationCode==%d\n", string(txidb), txIDIndex.TxValidationCode)
			//block
			blkLoc := &index.FileLocPointer{}
			blkLoc.Unmarshal(txIDIndex.BlkLocation)
			blkv, err := json.Marshal(blkLoc)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("blk Key[hxyz t64%s%d%d]=Value[%s]\n", string(txidb), blockNum2, blockNum3, string(blkv))
			//txid
			txLoc := &index.FileLocPointer{}
			txLoc.Unmarshal(txIDIndex.TxLocation)
			txv, err := json.Marshal(txLoc)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			//key == channel + " " + len(txid) + txid + blocknum + txNum
			fmt.Printf("tx Key[hxyz t64%s%d%d]=Value[%s]\n", string(txidb), blockNum2, blockNum3, string(txv))
			fmt.Println("\n")
		}
		var i bytes.Buffer
		i.WriteString(channel)
		i.WriteByte(0)
		i.WriteRune('i')
		prefix_i := i.String()
		if strings.HasPrefix(key, prefix_i) {
			keyss := iter.Key()
			//fmt.Println("key==", string(keyss))
			value := index.DecodeBlockNum(iter.Value())
			fmt.Printf("Key[%s]=Value[%d]\n", string(keyss), value)
		}
		var b bytes.Buffer
		b.WriteString(channel)
		b.WriteByte(0)
		b.WriteRune('b')
		prefix_b := b.String()
		if strings.HasPrefix(key, prefix_b) {
			keyss := iter.Key()
			//fmt.Println("key==", string(keyss))
			i := &index.BlockfilesInfo{}
			if err := i.Unmarshal(iter.Value()); err != nil {
				fmt.Println("blockfilesInfo", err)
			}
			val, err := json.Marshal(i)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("Key[%s]=Value[%s]\n", string(keyss), string(val))
		}
	}
	iter.Release()
	//err := iter.Error()
}

//n
type blockfileStream struct {
	fileNum       int
	file          *os.File
	reader        *bufio.Reader
	currentOffset int64
}

type blockPlacementInfo struct {
	fileNum          int
	blockStartOffset int64
	blockBytesOffset int64
}

func (s *blockfileStream) close() error {
	return errors.WithStack(s.file.Close())
}

func deriveBlockfilePath(rootDir string, suffixNum int) string {
	return rootDir + "/" + index.BlockfilePrefix + fmt.Sprintf("%06d", suffixNum)
}

func newBlockfileStream(rootDir string, fileNum int, startOffset int64) (*blockfileStream, error) {
	filePath := deriveBlockfilePath(rootDir, fileNum)
	var file *os.File
	var err error
	if file, err = os.OpenFile(filePath, os.O_RDONLY, 0600); err != nil {
		return nil, errors.Wrapf(err, "error opening block file %s", filePath)
	}
	var newPosition int64
	if newPosition, err = file.Seek(startOffset, 0); err != nil {
		return nil, errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", filePath, startOffset)
	}
	if newPosition != startOffset {
		panic(fmt.Sprintf("Could not seek block file [%s] to startOffset [%d]. New position = [%d]",
			filePath, startOffset, newPosition))
	}
	s := &blockfileStream{fileNum, file, bufio.NewReader(file), startOffset}
	return s, nil
}

func (s *blockfileStream) nextBlockBytesAndPlacementInfo() ([]byte, *blockPlacementInfo, error) {
	var lenBytes []byte
	var err error
	var fileInfo os.FileInfo
	moreContentAvailable := true

	if fileInfo, err = s.file.Stat(); err != nil {
		return nil, nil, errors.Wrapf(err, "error getting block file stat")
	}
	if s.currentOffset == fileInfo.Size() {
		fmt.Printf("Finished reading file number [%d]\n", s.fileNum)
		return nil, nil, nil
	}
	remainingBytes := fileInfo.Size() - s.currentOffset
	// Peek 8 or smaller number of bytes (if remaining bytes are less than 8)
	// Assumption is that a block size would be small enough to be represented in 8 bytes varint
	peekBytes := 8
	if remainingBytes < int64(peekBytes) {
		peekBytes = int(remainingBytes)
		moreContentAvailable = false
	}
	fmt.Printf("Remaining bytes=[%d], Going to peek [%d] bytes\n", remainingBytes, peekBytes)
	if lenBytes, err = s.reader.Peek(peekBytes); err != nil {
		return nil, nil, errors.Wrapf(err, "error peeking [%d] bytes from block file", peekBytes)
	}
	length, n := proto.DecodeVarint(lenBytes)
	if n == 0 {
		// proto.DecodeVarint did not consume any byte at all which means that the bytes
		// representing the size of the block are partial bytes
		if !moreContentAvailable {
			return nil, nil, err
		}
		panic(errors.Errorf("Error in decoding varint bytes [%#v]", lenBytes))
	}
	bytesExpected := int64(n) + int64(length)
	if bytesExpected > remainingBytes {
		fmt.Printf("At least [%d] bytes expected. Remaining bytes = [%d]. Returning with error [%s]",
			bytesExpected, remainingBytes, err)
		return nil, nil, err
	}
	// skip the bytes representing the block size
	if _, err = s.reader.Discard(n); err != nil {
		return nil, nil, errors.Wrapf(err, "error discarding [%d] bytes", n)
	}
	blockBytes := make([]byte, length)
	if _, err = io.ReadAtLeast(s.reader, blockBytes, int(length)); err != nil {
		fmt.Printf("Error reading [%d] bytes from file number [%d], error: %s", length, s.fileNum, err)
		return nil, nil, errors.Wrapf(err, "error reading [%d] bytes from file number [%d]", length, s.fileNum)
	}
	blockPlacementInfo := &blockPlacementInfo{
		fileNum:          s.fileNum,
		blockStartOffset: s.currentOffset,
		blockBytesOffset: s.currentOffset + int64(n)}
	s.currentOffset += int64(n) + int64(length)
	fmt.Printf("Returning blockbytes - length=[%d], placementInfo={%s}", len(blockBytes), blockPlacementInfo)
	return blockBytes, blockPlacementInfo, nil
}

func (s *blockfileStream) nextBlockBytes() ([]byte, error) {
	blockBytes, _, err := s.nextBlockBytesAndPlacementInfo()
	return blockBytes, err
}

type buffer struct {
	buf      *proto.Buffer
	position int
}

func newBuffer(b []byte) *buffer {
	return &buffer{proto.NewBuffer(b), 0}
}

// DecodeVarint wraps the actual method and updates the position
func (b *buffer) DecodeVarint() (uint64, error) {
	val, err := b.buf.DecodeVarint()
	if err == nil {
		b.position += proto.SizeVarint(val)
	} else {
		err = errors.Wrap(err, "error decoding varint with proto.Buffer")
	}
	return val, err
}

// DecodeRawBytes wraps the actual method and updates the position
func (b *buffer) DecodeRawBytes(alloc bool) ([]byte, error) {
	val, err := b.buf.DecodeRawBytes(alloc)
	if err == nil {
		b.position += proto.SizeVarint(uint64(len(val))) + len(val)
	} else {
		err = errors.Wrap(err, "error decoding raw bytes with proto.Buffer")
	}
	return val, err
}

// GetBytesConsumed returns the offset of the current position in the underlying []byte
func (b *buffer) GetBytesConsumed() int {
	return b.position
}

func extractHeader(buf *buffer) (*common.BlockHeader, error) {
	header := &common.BlockHeader{}
	var err error
	if header.Number, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "error decoding the block number")
	}
	if header.DataHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "error decoding the data hash")
	}
	if header.PreviousHash, err = buf.DecodeRawBytes(false); err != nil {
		return nil, errors.Wrap(err, "error decoding the previous hash")
	}
	if len(header.PreviousHash) == 0 {
		header.PreviousHash = nil
	}
	return header, nil
}

type txindexInfo struct {
	txID string
	loc  *index.LocPointer
}

func extractData(buf *buffer) (*common.BlockData, []*txindexInfo, error) {
	data := &common.BlockData{}
	var txOffsets []*txindexInfo
	var numItems uint64
	var err error

	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, nil, errors.Wrap(err, "error decoding the length of block data")
	}
	for i := uint64(0); i < numItems; i++ {
		var txEnvBytes []byte
		var txid string
		txOffset := buf.GetBytesConsumed()
		if txEnvBytes, err = buf.DecodeRawBytes(false); err != nil {
			return nil, nil, errors.Wrap(err, "error decoding the transaction envelope")
		}
		if txid, err = protoutil.GetOrComputeTxIDFromEnvelope(txEnvBytes); err != nil {
			fmt.Printf("error while extracting txid from tx envelope bytes during deserialization of block. Ignoring this error as this is caused by a malformed transaction. Error:%s",
				err)

		}
		data.Data = append(data.Data, txEnvBytes)
		idxInfo := &txindexInfo{txID: txid, loc: &index.LocPointer{txOffset, buf.GetBytesConsumed() - txOffset}}
		txOffsets = append(txOffsets, idxInfo)
	}
	return data, txOffsets, nil
}

func extractMetadata(buf *buffer) (*common.BlockMetadata, error) {
	metadata := &common.BlockMetadata{}
	var numItems uint64
	var metadataEntry []byte
	var err error
	if numItems, err = buf.DecodeVarint(); err != nil {
		return nil, errors.Wrap(err, "error decoding the length of block metadata")
	}
	for i := uint64(0); i < numItems; i++ {
		if metadataEntry, err = buf.DecodeRawBytes(false); err != nil {
			return nil, errors.Wrap(err, "error decoding the block metadata")
		}
		metadata.Metadata = append(metadata.Metadata, metadataEntry)
	}
	return metadata, nil
}

func deserializeBlock(serializedBlockBytes []byte) (*common.Block, error) {
	block := &common.Block{}
	var err error
	b := newBuffer(serializedBlockBytes)
	if block.Header, err = extractHeader(b); err != nil {
		return nil, err
	}
	if block.Data, _, err = extractData(b); err != nil {
		return nil, err
	}
	if block.Metadata, err = extractMetadata(b); err != nil {
		return nil, err
	}
	return block, nil
}

func GetBlockLocByBlockNum(db *leveldb.DB, channel, peerSrcPath string, blockNum uint64) (*index.FileLocPointer, error) {
	blockNumKey := index.ConstructBlockNumKey(blockNum)
	blockKey := index.ConstructLevelKey(channel, blockNumKey)
	value, err := db.Get(blockKey, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	if value == nil {
		fmt.Println("getBlockLocByBlockNum===nil")
	}
	blkLoc := &index.FileLocPointer{}
	blkLoc.Unmarshal(value)
	v, err := json.Marshal(blkLoc)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===", string(v))
	//get block
	//rootDir := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/chains/chains/hxyz"
	rootDir := fmt.Sprintf("%s/chains/%s", peerSrcPath, channel)
	stream, err := newBlockfileStream(rootDir, blkLoc.FileSuffixNum, int64(blkLoc.Offset))
	if err != nil {
		return nil, err
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	block, err := deserializeBlock(b)
	if err != nil {
		return nil, err
	}

	//get block hash
	blockHash := protoutil.BlockHeaderHash(block.Header)
	fmt.Println("blockHash===", string(blockHash))
	return blkLoc, nil
}

func UpdateBlockLocByBlockNum(db *leveldb.DB, channel string, blockNum uint64, fileSuffixNum, offset, bytesLength int) error {
	blockNumKey := index.ConstructBlockNumKey(blockNum)
	blockKey := index.ConstructLevelKey(channel, blockNumKey)
	flp := &index.FileLocPointer{}
	flp.FileSuffixNum = fileSuffixNum
	flp.Offset = offset
	flp.BytesLength = bytesLength

	value, err := flp.Marshal()
	if err != nil {
		return err
	}
	err = db.Put(blockKey, value, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}

// h
func constructBlockHashKey(blockHash []byte) []byte {
	return append([]byte{index.BlockHashIdxKeyPrefix}, blockHash...)
}

func GetBlockHashByBlockNum(db *leveldb.DB, channel, peerSrcPath string, blockNum uint64) []byte {
	blockNumKey := index.ConstructBlockNumKey(blockNum)
	blockKey := index.ConstructLevelKey(channel, blockNumKey)
	value, err := db.Get(blockKey, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	if value == nil {
		fmt.Println("getBlockLocByBlockNum===nil")
	}
	blkLoc := &index.FileLocPointer{}
	blkLoc.Unmarshal(value)
	v, err := json.Marshal(blkLoc)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===", string(v))
	//get block
	//rootDir := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/chains/chains/hxyz"
	rootDir := fmt.Sprintf("%s/chains/%s", peerSrcPath, channel)
	stream, err := newBlockfileStream(rootDir, blkLoc.FileSuffixNum, int64(blkLoc.Offset))
	if err != nil {
		fmt.Println("err", err)
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		fmt.Println("err", err)
	}
	block, err := deserializeBlock(b)
	if err != nil {
		fmt.Println("err", err)
	}

	//get block hash
	blockHash := protoutil.BlockHeaderHash(block.Header)
	fmt.Println("blockHash===", string(blockHash))
	testHash := hex.EncodeToString(blockHash)
	fmt.Println("blockHash===", hex.EncodeToString(blockHash))
	testBy, _ := hex.DecodeString(testHash)
	fmt.Println("blockHash===", string(testBy))
	return blockHash
}

func GetBlockLocByHash(db *leveldb.DB, channel string, hash []byte) (*index.FileLocPointer, error) {
	hashBytes := constructBlockHashKey(hash)
	hashKey := index.ConstructLevelKey(channel, hashBytes)
	fmt.Println("GetBlockLocByHash key==", string(hashKey))
	value, err := db.Get(hashKey, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	if value == nil {
		fmt.Println("getBlockLocByBlockNum===nil")
	}

	blkLoc := &index.FileLocPointer{}
	blkLoc.Unmarshal(value)
	v, err := json.Marshal(blkLoc)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===", string(v))
	return blkLoc, nil
}

func UpdateBlockByHash(db *leveldb.DB, channel string, hash []byte, fileSuffixNum, offset, bytesLength int) error {
	hashKey := constructBlockHashKey(hash)
	key := index.ConstructLevelKey(channel, hashKey)
	flp := &index.FileLocPointer{}
	flp.FileSuffixNum = fileSuffixNum
	flp.Offset = offset
	flp.BytesLength = bytesLength

	value, err := flp.Marshal()
	if err != nil {
		return err
	}
	err = db.Put(key, value, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}

//t

func GetBlockTxidByBlockNum(db *leveldb.DB, channel string, blockNum uint64) []string {
	blockNumKey := index.ConstructBlockNumKey(blockNum)
	blockKey := index.ConstructLevelKey(channel, blockNumKey)
	value, err := db.Get(blockKey, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
	}
	if value == nil {
		fmt.Println("getBlockLocByBlockNum===nil")
	}
	blkLoc := &index.FileLocPointer{}
	blkLoc.Unmarshal(value)
	v, err := json.Marshal(blkLoc)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===", string(v))
	//get block
	//rootDir := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/chains/chains/hxyz"
	rootDir := fmt.Sprintf("%s/chains/%s", config.Conf.Path.PeerSrcPath, config.Conf.Path.Channel)
	stream, err := newBlockfileStream(rootDir, blkLoc.FileSuffixNum, int64(blkLoc.Offset))
	if err != nil {
		fmt.Println("err", err)
	}
	defer stream.close()
	b, err := stream.nextBlockBytes()
	if err != nil {
		fmt.Println("err", err)
	}
	block, err := deserializeBlock(b)
	if err != nil {
		fmt.Println("err", err)
	}

	//get txid
	var txidArr []string
	txidNum := len(block.Data.Data)
	for i := 0; i < txidNum; i++ {
		txid, err := protoutil.GetOrComputeTxIDFromEnvelope(block.Data.Data[i])
		if err != nil {
			fmt.Println("err", err)
		}
		fmt.Println("txid==\n", txid)
		txidArr = append(txidArr, txid)
	}

	return txidArr
}

type rangeScan struct {
	startKey []byte
	stopKey  []byte
}

func constructTxIDRangeScan(txID string) *rangeScan {
	sk := append(
		[]byte{index.TxIDIdxKeyPrefix},
		util.EncodeOrderPreservingVarUint64(uint64(len(txID)))...,
	)
	sk = append(sk, txID...)
	return &rangeScan{
		startKey: sk,
		stopKey:  append(sk, 0xff),
	}
}

type Iterator struct {
	dbName string
	iterator.Iterator
}

func constructTxIDKey(txID string, blkNum, txNum uint64) []byte {
	k := append(
		[]byte{index.TxIDIdxKeyPrefix},
		util.EncodeOrderPreservingVarUint64(uint64(len(txID)))...,
	)
	k = append(k, txID...)
	k = append(k, util.EncodeOrderPreservingVarUint64(blkNum)...)
	return append(k, util.EncodeOrderPreservingVarUint64(txNum)...)
}

func GetBlockLocByTxID(db *leveldb.DB, channel string, txID string) (*index.FileLocPointer, *index.FileLocPointer, error) {
	rangeScan := constructTxIDRangeScan(txID)
	sKey := index.ConstructLevelKey(channel, rangeScan.startKey)
	eKey := index.ConstructLevelKey(channel, rangeScan.stopKey)
	if rangeScan.stopKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = index.LastKeyIndicator
	}
	//fmt.Printf("Getting iterator for range [%#v] - [%#v]", sKey, eKey)
	itr := db.NewIterator(&goleveldbutil.Range{Start: sKey, Limit: eKey}, &opt.ReadOptions{})
	iitr := &Iterator{channel, itr}
	defer iitr.Release()

	present := iitr.Next()
	if err := iitr.Error(); err != nil {
		return nil, nil, fmt.Errorf("error: %s error while trying to retrieve transaction info by TXID [%s]", err, txID)
	}
	if !present {
		return nil, nil, fmt.Errorf("not found value")
	}
	valBytes := itr.Value()
	if len(valBytes) == 0 {
		return nil, nil, fmt.Errorf("itr:len(valBytes) == 0")
	}
	val := &index.TxIDIndexValue{}
	if err := proto.Unmarshal(valBytes, val); err != nil {
		return nil, nil, fmt.Errorf("unexpected error while unmarshaling bytes [%#v] into TxIDIndexValProto", valBytes)
	}
	blkFLP := &index.FileLocPointer{}
	if err := blkFLP.Unmarshal(val.BlkLocation); err != nil {
		return nil, nil, fmt.Errorf("GetBlockLocByTxID err=%s", err)
	}
	v, err := json.Marshal(blkFLP)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===\n", string(v))
	txFLP := &index.FileLocPointer{}
	if err := txFLP.Unmarshal(val.TxLocation); err != nil {
		return nil, nil, fmt.Errorf("GetBlockLocByTxID err=%s", err)
	}
	return blkFLP, txFLP, nil
}

func UpdateBlockAndTxLocByTxID(db *leveldb.DB, channel string, blockNum uint64, txid string, bfileSuffixNum, boffset, bbytesLength, tfileSuffixNum, toffset, tbytesLength int) error {

	blkflp := &index.FileLocPointer{}
	blkflp.FileSuffixNum = bfileSuffixNum
	blkflp.Offset = boffset
	blkflp.BytesLength = bbytesLength

	txflpp := &index.FileLocPointer{}
	txflpp.FileSuffixNum = tfileSuffixNum
	txflpp.Offset = toffset
	txflpp.BytesLength = tbytesLength
	flpBytes, err := blkflp.Marshal()
	if err != nil {
		return err
	}
	txFlpBytes, err := txflpp.Marshal()
	if err != nil {
		return err
	}
	indexVal := &index.TxIDIndexValue{
		BlkLocation:      flpBytes,
		TxLocation:       txFlpBytes,
		TxValidationCode: int32(0),
	}
	indexValBytes, err := proto.Marshal(indexVal)
	if err != nil {
		return errors.Wrap(err, "unexpected error while marshaling TxIDIndexValProto message")
	}

	txKey := constructTxIDKey(txid, blockNum, uint64(0))
	key := index.ConstructLevelKey(channel, txKey)
	err = db.Put(key, indexValBytes, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}

// a

func constructBlockNumTranNumKey(blockNum uint64, txNum uint64) []byte {
	blkNumBytes := util.EncodeOrderPreservingVarUint64(blockNum)
	tranNumBytes := util.EncodeOrderPreservingVarUint64(txNum)
	key := append(blkNumBytes, tranNumBytes...)
	return append([]byte{index.BlockNumTranNumIdxKeyPrefix}, key...)
}

func GetTXLocByBlockNumTranNum(db *leveldb.DB, channel string, blockNum uint64, tranNum uint64) (*index.FileLocPointer, error) {
	keys := constructBlockNumTranNumKey(blockNum, tranNum)
	levelKey := index.ConstructLevelKey(channel, keys)
	value, err := db.Get(levelKey, &opt.ReadOptions{})
	if err != nil {
		fmt.Println("err", err)
		panic(err)
	}
	if value == nil {
		return nil, err
	}
	txFLP := &index.FileLocPointer{}
	txFLP.Unmarshal(value)
	v, err := json.Marshal(txFLP)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===", string(v))
	return txFLP, nil

}

func UpdateTxLocByBlockNumTranNum(db *leveldb.DB, channel string, blockNum, tranNum uint64, fileSuffixNum, offset, bytesLength int) error {
	txkflp := &index.FileLocPointer{}
	txkflp.FileSuffixNum = fileSuffixNum
	txkflp.Offset = offset
	txkflp.BytesLength = bytesLength

	txFlpBytes, err := txkflp.Marshal()
	if err != nil {
		return err
	}
	keys := constructBlockNumTranNumKey(blockNum, tranNum)
	key := index.ConstructLevelKey(channel, keys)
	err = db.Put(key, txFlpBytes, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}
