package pvdataStore

import (
	"bytes"
	"encoding/binary"
	"math"
	historyledger "testLevelDB/historyLedger"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/willf/bitset"
)

var (
	pendingCommitKey                 = []byte{0}
	lastCommittedBlkkey              = []byte{1}
	PvtDataKeyPrefix                 = []byte{2}
	expiryKeyPrefix                  = []byte{3}
	elgPrioritizedMissingDataGroup   = []byte{4}
	inelgMissingDataGroup            = []byte{5}
	collElgKeyPrefix                 = []byte{6}
	lastUpdatedOldBlocksKey          = []byte{7}
	elgDeprioritizedMissingDataGroup = []byte{8}

	nilByte    = byte(0)
	emptyValue = []byte{}
)

type NsCollBlk struct {
	Ns, Coll string
	BlkNum   uint64
}

type DataKey struct {
	NsCollBlk
	TxNum uint64
}

type dataEntry struct {
	key   *DataKey
	value *rwset.CollectionPvtReadWriteSet
}

func DecodeReverseOrderVarUint64(bytes []byte) (uint64, int) {
	s, _ := proto.DecodeVarint(bytes)
	numFFBytes := int(s)
	decodedBytes := make([]byte, 8)
	realBytesNum := 8 - numFFBytes
	copy(decodedBytes[numFFBytes:], bytes[1:realBytesNum+1])
	numBytesConsumed := realBytesNum + 1
	for i := 0; i < numFFBytes; i++ {
		decodedBytes[i] = 0xff
	}
	return (math.MaxUint64 - binary.BigEndian.Uint64(decodedBytes)), numBytesConsumed
}

func EncodeMissingDataValue(bitmap *bitset.BitSet) ([]byte, error) {
	return bitmap.MarshalBinary()
}

func DecodeMissingDataValue(bitmapBytes []byte) (*bitset.BitSet, error) {
	bitmap := &bitset.BitSet{}
	if err := bitmap.UnmarshalBinary(bitmapBytes); err != nil {
		return nil, err
	}
	return bitmap, nil
}

func DecodeDatakey(datakeyBytes []byte) (*DataKey, error) {
	v, n, err := historyledger.NewHeightFromBytes(datakeyBytes[1:])
	if err != nil {
		return nil, err
	}
	blkNum := v.BlockNum
	tranNum := v.TxNum
	remainingBytes := datakeyBytes[n+1:]
	nilByteIndex := bytes.IndexByte(remainingBytes, nilByte)
	ns := string(remainingBytes[:nilByteIndex])
	coll := string(remainingBytes[nilByteIndex+1:])
	return &DataKey{NsCollBlk{ns, coll, blkNum}, tranNum}, nil
}

func DecodeDataValue(datavalueBytes []byte) (*rwset.CollectionPvtReadWriteSet, error) {
	collPvtdata := &rwset.CollectionPvtReadWriteSet{}
	err := proto.Unmarshal(datavalueBytes, collPvtdata)
	return collPvtdata, err
}

func EncodeDataKey(key *DataKey) []byte {
	dataKeyBytes := append(PvtDataKeyPrefix, historyledger.NewHeight(key.BlkNum, key.TxNum).ToBytes()...)
	dataKeyBytes = append(dataKeyBytes, []byte(key.Ns)...)
	dataKeyBytes = append(dataKeyBytes, nilByte)
	return append(dataKeyBytes, []byte(key.Coll)...)
}
