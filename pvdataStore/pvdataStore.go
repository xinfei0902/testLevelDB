package pvdataStore

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
)

func ParseAllPvdataStoreKV(db *leveldb.DB, channel string) error {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		keys := iter.Key()

		clen := len(channel)
		flageKey := keys[clen+1 : clen+2] //parseKey 是一个字节标识
		if bytes.Compare(flageKey, lastCommittedBlkkey) == 0 {
			value, _ := proto.DecodeVarint(iter.Value())
			fmt.Printf("kev[hxyz %s]=value[%d]\n", flageKey, value)
		} else if bytes.Compare(flageKey, inelgMissingDataGroup) == 0 {
			parseKey := keys[clen+2:]
			ns := parseKey[:10]
			keyColl := parseKey[11:32]
			blockNum, _ := DecodeReverseOrderVarUint64(parseKey[33:])
			//fmt.Println(string(iter.Value()))
			value, err := DecodeMissingDataValue(iter.Value())
			if err != nil {
				return err
			}
			length := value.Len()

			set := value.Bytes()
			//vbit := value.String()
			//fmt.Println("vbit=", vbit, length, len(set), set)
			// v, err := json.Marshal(value)
			// if err != nil {
			// 	fmt.Println("json marshal err", err)
			// }
			fmt.Printf("key[%s %s%s %s %d]=value[{length:%d,set:%d}]\n", channel, string(flageKey), ns, keyColl, blockNum, length, set)
		} else if bytes.Compare(flageKey, PvtDataKeyPrefix) == 0 {
			//parseKey := keys[clen+2:]
			data, err := DecodeDatakey(keys[clen+1:])
			if err != nil {
				return err
			}
			value, err := DecodeDataValue(iter.Value())
			if err != nil {
				return err
			}

			// dataEntryInfo := &dataEntry{
			// 	key:   data,
			// 	value: value,
			// }
			// dataEntryArrary := []*dataEntry{}
			// dataEntryArrary = append(dataEntryArrary, dataEntryInfo)

			// nsPvtReadWriteSet := []*rwset.NsPvtReadWriteSet{
			// 	{
			// 		Namespace:          data.ns,
			// 		CollectionPvtRwset: []*rwset.CollectionPvtReadWriteSet{value},
			// 	},
			// }
			// writeSet := &rwset.TxPvtReadWriteSet{
			// 	DataModel:  1,
			// 	NsPvtRwset: nsPvtReadWriteSet,
			// }
			// pvtDataArrary := []*ledger.TxPvtData{}
			// pvtData := &ledger.TxPvtData{
			// 	SeqInBlock: data.blkNum,
			// 	WriteSet:   writeSet,
			// }
			// pvtDataArrary = append(pvtDataArrary, pvtData)
			v, err := json.Marshal(value)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("key[%s %s%d%d %s %s]=value[%s]\n", channel, string(flageKey), data.BlkNum, data.TxNum, data.Ns, data.Coll, string(v))
		}

	}
	return nil
}
