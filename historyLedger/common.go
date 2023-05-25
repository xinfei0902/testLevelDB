package historyledger

import (
	"fmt"

	"github.com/hyperledger/fabric/common/ledger/util"
)

type Height struct {
	BlockNum uint64
	TxNum    uint64
}

// NewHeight constructs a new instance of Height
func NewHeight(blockNum, txNum uint64) *Height {
	return &Height{blockNum, txNum}
}

// NewHeightFromBytes constructs a new instance of Height from serialized bytes
func NewHeightFromBytes(b []byte) (*Height, int, error) {
	blockNum, n1, err := util.DecodeOrderPreservingVarUint64(b)
	if err != nil {
		return nil, -1, err
	}
	txNum, n2, err := util.DecodeOrderPreservingVarUint64(b[n1:])
	if err != nil {
		return nil, -1, err
	}
	return NewHeight(blockNum, txNum), n1 + n2, nil
}

func (h *Height) String() string {
	return fmt.Sprintf("{BlockNum: %d, TxNum: %d}", h.BlockNum, h.TxNum)
}

func (h *Height) ToBytes() []byte {
	blockNumBytes := util.EncodeOrderPreservingVarUint64(h.BlockNum)
	txNumBytes := util.EncodeOrderPreservingVarUint64(h.TxNum)
	return append(blockNumBytes, txNumBytes...)
}
