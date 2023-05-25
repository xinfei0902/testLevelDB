package orderer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testLevelDB/index"

	"github.com/hyperledger/fabric/common/ledger/util"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// all
func ReadOrdererAllIndex(db *leveldb.DB, channel string) {

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		var n bytes.Buffer
		n.WriteString("hxyz")
		n.WriteByte(0)
		n.WriteRune('n')
		prefix_n := n.String()
		if strings.HasPrefix(key, prefix_n) {
			keyss := iter.Key()
			// if len(keyss[7:]) == 0 {
			//blockNum6, _ := proto.DecodeVarint(keyss[6:])
			blockNum6, _, err := util.DecodeOrderPreservingVarUint64(keyss[6:])
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			blkLoc := &index.FileLocPointer{}
			blkLoc.Unmarshal(iter.Value())
			v, err := json.Marshal(blkLoc)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("Key[hxyz n%d]=Value[%s]\n", blockNum6, string(v))
			// } else {
			// 	//blockNum6, _ := proto.DecodeVarint(keyss[6:7])
			// 	blockNum7, _ := proto.DecodeVarint(keyss[7:8])
			// 	blkLoc := &index.FileLocPointer{}
			// 	blkLoc.Unmarshal(iter.Value())
			// 	v, err := json.Marshal(blkLoc)
			// 	if err != nil {
			// 		fmt.Println("json marshal err", err)
			// 	}
			// 	fmt.Printf("Key[hxyz n%d]=Value[%s]\n", blockNum7, string(v))
			// }

		}
		var i bytes.Buffer
		i.WriteString("hxyz")
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
		b.WriteString("hxyz")
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

		var sn bytes.Buffer
		sn.WriteString("system-channel")
		sn.WriteByte(0)
		sn.WriteRune('n')
		prefix_sn := sn.String()
		if strings.HasPrefix(key, prefix_sn) {
			keyss := iter.Key()
			//if len(keyss[17:]) == 0 {
			//blockNum16, _ := proto.DecodeVarint(keyss[16:])
			blockNum16, _, err := util.DecodeOrderPreservingVarUint64(keyss[16:])
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			blkLoc := &index.FileLocPointer{}
			blkLoc.Unmarshal(iter.Value())
			v, err := json.Marshal(blkLoc)
			if err != nil {
				fmt.Println("json marshal err", err)
			}
			fmt.Printf("Key[system-channel n%d]=Value[%s]\n", blockNum16, string(v))
			// } else {
			// 	//blockNum6, _ := proto.DecodeVarint(keyss[6:7])
			// 	blockNum17, _ := proto.DecodeVarint(keyss[17:18])
			// 	blkLoc := &index.FileLocPointer{}
			// 	blkLoc.Unmarshal(iter.Value())
			// 	v, err := json.Marshal(blkLoc)
			// 	if err != nil {
			// 		fmt.Println("json marshal err", err)
			// 	}
			// 	fmt.Printf("Key[system-channel n%d]=Value[%s]\n", blockNum17, string(v))
			// }

		}
		var si bytes.Buffer
		si.WriteString("system-channel")
		si.WriteByte(0)
		si.WriteRune('i')
		prefix_si := si.String()
		if strings.HasPrefix(key, prefix_si) {
			keyss := iter.Key()
			//fmt.Println("key==", string(keyss))
			value := index.DecodeBlockNum(iter.Value())
			fmt.Printf("Key[%s]=Value[%d]\n", string(keyss), value)
		}
		var sb bytes.Buffer
		sb.WriteString("system-channel")
		sb.WriteByte(0)
		sb.WriteRune('b')
		prefix_sb := sb.String()
		if strings.HasPrefix(key, prefix_sb) {
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

//
func GetBlockMgrInfo(db *leveldb.DB, channel string) (*index.BlockfilesInfo, error) {
	key := index.ConstructLevelKey(channel, index.BlkMgrInfoKey)
	value, err := db.Get(key, &opt.ReadOptions{})
	if err != nil {
		return nil, err
	}
	i := &index.BlockfilesInfo{}
	if err := i.Unmarshal(value); err != nil {
		fmt.Println("blockfilesInfo", err)
	}
	return i, nil
}

func GetBlockLocByBlockNum(db *leveldb.DB, channel string, blockNum uint64) (*index.FileLocPointer, error) {
	blockNumKey := index.ConstructBlockNumKey(blockNum)
	blockKey := index.ConstructLevelKey(channel, blockNumKey)
	value, err := db.Get(blockKey, &opt.ReadOptions{})
	if err != nil {
		return nil, err
	}
	blkLoc := &index.FileLocPointer{}
	blkLoc.Unmarshal(value)
	return blkLoc, nil
}

//n

func UpdateOrdererIndexByBlockNum(db *leveldb.DB, channel string, blockNum uint64, fileSuffixNum, offset, bytesLength int) error {
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
