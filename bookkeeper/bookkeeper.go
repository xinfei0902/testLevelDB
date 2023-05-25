package bookkeeper

import (
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/pvtstatepurgemgmt"
	"github.com/syndtr/goleveldb/leveldb"
)

type ExpiryInfo struct {
	ExpiryInfoKey *ExpiryInfoKey
	PvtdataKeys   *pvtstatepurgemgmt.PvtdataKeys
}

// expiryInfoKey is used as a key of an entry in the expiryKeeper (backed by a leveldb instance)
type ExpiryInfoKey struct {
	CommittingBlk uint64
	ExpiryBlk     uint64
}

func ParseAllBookkeeperKV(db *leveldb.DB, channel string) error {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		v, err := decodeExpiryInfo(key[7:], iter.Value())
		if err != nil {
			return err
		}
		value, _ := json.Marshal(v)
		fmt.Println(string(value))
	}

	return nil
}

func decodeExpiryInfo(key []byte, value []byte) (*ExpiryInfo, error) {
	expiryBlk, n, err := util.DecodeOrderPreservingVarUint64(key[1:])
	if err != nil {
		return nil, err
	}
	committingBlk, _, err := util.DecodeOrderPreservingVarUint64(key[n+1:])
	if err != nil {
		return nil, err
	}
	pvtdataKeys := &pvtstatepurgemgmt.PvtdataKeys{}
	if err := proto.Unmarshal(value, pvtdataKeys); err != nil {
		return nil, err
	}
	return &ExpiryInfo{
			ExpiryInfoKey: &ExpiryInfoKey{CommittingBlk: committingBlk, ExpiryBlk: expiryBlk},
			PvtdataKeys:   pvtdataKeys},
		nil
}
