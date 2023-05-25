package stateleveldb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	historyledger "testLevelDB/historyLedger"
	"testLevelDB/index"

	"github.com/golang/protobuf/proto"
	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/stateleveldb"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	DataKeyPrefix               = []byte{'d'}
	dataKeyStopper              = []byte{'e'}
	nsKeySep                    = []byte{0x00}
	lastKeyIndicator            = byte(0x01)
	savePointKey                = []byte{'s'}
	fullScanIteratorValueFormat = byte(1)
)

func EncodeDataKey(ns, key string) []byte {
	k := append(DataKeyPrefix, []byte(ns)...)
	k = append(k, nsKeySep...)
	return append(k, []byte(key)...)
}

func decodeDataKey(encodedDataKey []byte) (string, string) {
	split := bytes.SplitN(encodedDataKey, nsKeySep, 2)
	return string(split[0][1:]), string(split[1])
}

func ParseAllStateLevelDBKV(db *leveldb.DB, channel string) error {
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		var d bytes.Buffer
		d.WriteString("hxyz")
		d.WriteByte(0)
		d.WriteRune('d')
		d.WriteString("_lifecycle$$h_implicit_org_Org1MSP")
		d.WriteByte(0)
		d.WriteString("?4s�")
		prefix_d := d.String()
		if strings.HasPrefix(key, prefix_d) {
			keyd := iter.Key()
			ns, ckey := decodeDataKey(keyd[6:])
			fmt.Printf("ns=%s,ckey=%s\n", ns, ckey)
			fmt.Printf("vv", string(iter.Value()))
		}

	}
	return nil
}

func GetPackIDInfoByKey(db *leveldb.DB, channel string) error {
	ns := "_lifecycle$$p_implicit_org_Org1MSP"
	ckey := "chaincode-sources/fields/archive#1/PackageID"
	cokey := EncodeDataKey(ns, ckey)
	key := index.ConstructLevelKey("hxyz", cokey)
	v, err := db.Get(key, &opt.ReadOptions{})
	if err != nil {
		return err
	}
	stateVV, err := decodeValue(v)
	if err != nil {
		return err
	}
	value, err := json.Marshal(stateVV)
	if err != nil {
		fmt.Println("json marshal err", err)
	}
	fmt.Println("value===\n", string(stateVV.Value))
	fmt.Println("value===\n", string(value))
	return nil
}

type VersionedValue struct {
	Value    []byte
	Metadata []byte
	Version  *historyledger.Height
}

func decodeValue(encodedValue []byte) (*VersionedValue, error) {
	dbValue := &stateleveldb.DBValue{}
	err := proto.Unmarshal(encodedValue, dbValue)
	if err != nil {
		return nil, err
	}
	ver, _, err := historyledger.NewHeightFromBytes(dbValue.Version)
	if err != nil {
		return nil, err
	}
	val := dbValue.Value
	metadata := dbValue.Metadata
	// protobuf always makes an empty byte array as nil
	if val == nil {
		val = []byte{}
	}
	return &VersionedValue{Version: ver, Value: val, Metadata: metadata}, nil
}

func encodeValue(v *VersionedValue) ([]byte, error) {
	return proto.Marshal(
		&stateleveldb.DBValue{
			Version:  v.Version.ToBytes(),
			Value:    v.Value,
			Metadata: v.Metadata,
		},
	)
}

func UpdateCCPackageID(db *leveldb.DB, channel, ns, ckey, packageID string) error {
	// ns := "_lifecycle$$p_implicit_org_Org1MSP"
	// ckey := "chaincode-sources/fields/archive#1/PackageID"
	cokey := EncodeDataKey(ns, ckey)
	key := index.ConstructLevelKey(channel, cokey)
	//vv := fmt.Sprintf("L%s", packageID)
	//value := []byte("Larchive_1.0:cb2d1db4248398c0f567bb0787ea4cb7bea6ccf010b31e67df81d9f4af09c762")
	//fmt.Println("b", string(value))
	encodedCCHash := protoutil.MarshalOrPanic(&lb.StateData{
		Type: &lb.StateData_String_{String_: packageID},
	})
	height := &historyledger.Height{
		BlockNum: 3,
		TxNum:    0,
	}
	versionValue := &VersionedValue{
		Value:   encodedCCHash,
		Version: height,
	}
	v, err := encodeValue(versionValue)
	if err != nil {
		return err
	}
	err = db.Put(key, v, &opt.WriteOptions{})
	if err != nil {
		return err
	}
	return nil
}
