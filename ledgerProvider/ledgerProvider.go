package ledgerprovider

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/core/ledger/kvledger/msgs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func ParseAllLedgerProviderKV(db *leveldb.DB, channel string) error {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		if strings.HasPrefix(key, string(ledgerKeyPrefix)) {
			blockValue := &common.Block{}
			err := proto.Unmarshal(iter.Value(), blockValue)
			if err != nil {
				return err
			}

			v, err := json.Marshal(blockValue)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("key[%s]=value[%s]\n", key, string(v))
		} else if strings.HasPrefix(key, string(metadataKeyPrefix)) { //账本状态  0-正常  1-不正常
			metadata := &msgs.LedgerMetadata{}
			err := proto.Unmarshal(iter.Value(), metadata)
			if err != nil {
				return err
			}
			v, err := json.Marshal(metadata)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("key[%s]=value[%s]\n", key, string(v))
		}
	}
	return nil
}

//
func UpdateLedgerProviderBlock(db *leveldb.DB, channel string) error {

	return nil
}

//更新账本状态
func UpdateLedgerProviderStatus(db *leveldb.DB, channel string) error {
	metadata, err := proto.Marshal(&msgs.LedgerMetadata{Status: msgs.Status_ACTIVE})
	if err != nil {
		return err
	}
	metadataKey := EncodeLedgerKey(channel, metadataKeyPrefix)
	err = db.Put(metadataKey, metadata, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}
