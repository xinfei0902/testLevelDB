package historyledger

import (
	"bytes"
	"fmt"
	"strings"
	"testLevelDB/index"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var emptyValue = []byte{}

func GetAllHistoryLedgerKV(db *leveldb.DB, channel string) error {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		var a bytes.Buffer
		a.WriteString(channel)
		a.WriteByte(0)
		a.WriteRune('s')
		prefix_a := a.String()
		if strings.HasPrefix(key, prefix_a) {
			height, _, err := NewHeightFromBytes(iter.Value())
			if err != nil {
				return err
			}
			value := height.String()
			//value 是区块高度和当前区块交易总个数
			fmt.Printf("kev[hxyz s]==value[%s]", value)
		}
		var l bytes.Buffer
		l.WriteString(channel)
		l.WriteByte(0)
		l.WriteRune('_')
		prefix_l := l.String()
		if strings.HasPrefix(key, prefix_l) {
			keys := iter.Key()
			leng := len(channel) + 12
			rwsetKeylen, i, err := util.DecodeOrderPreservingVarUint64(keys[leng:18])
			if err != nil {
				return nil
			}
			rwsetKeyStat := leng + i
			rwsetKeyEnd := rwsetKeyStat + int(rwsetKeylen)
			rwsetKey := keys[rwsetKeyStat:rwsetKeyEnd]
			blockNum, n, err := util.DecodeOrderPreservingVarUint64(keys[rwsetKeyEnd+1 : rwsetKeyEnd+3])
			if err != nil {
				return nil
			}
			txNum, _, err := util.DecodeOrderPreservingVarUint64(keys[rwsetKeyEnd+n+1:])
			if err != nil {
				return nil
			}
			fmt.Printf("kev[hxyz _lifecycle %d%s %d%d]==value[nil]\n", rwsetKeylen, string(rwsetKey), blockNum, txNum)
		}

	}
	return nil
}

func UpdateHistoryLedger(db *leveldb.DB, channel, ns, rwsetKey string, blockNo, tranNo uint64) error {
	switch ns {
	case "_lifecycle":
		dataKey := index.ConstructDataKey(ns, rwsetKey, blockNo, tranNo)
		key := index.ConstructLevelKey(channel, dataKey)
		err := db.Put(key, emptyValue, &opt.WriteOptions{})
		if err != nil {
			return err
		}
	case "s":
		height := NewHeight(blockNo, tranNo)
		key := index.ConstructLevelKey(channel, []byte(ns))
		err := db.Put(key, height.ToBytes(), &opt.WriteOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}
