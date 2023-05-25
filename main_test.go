package main

import (
	"encoding/hex"
	"fmt"
	"testLevelDB/bookkeeper"
	pHistory "testLevelDB/configHistory/peer"
	historyledger "testLevelDB/historyLedger"
	"testLevelDB/index"
	oIndex "testLevelDB/index/orderer"
	pIndex "testLevelDB/index/peer"
	ledgerProvider "testLevelDB/ledgerProvider"
	pvdataStore "testLevelDB/pvdataStore"
	stateLeveDB "testLevelDB/stateLevelDB"
	"testing"

	lb "github.com/hyperledger/fabric-protos-go/peer/lifecycle"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/tjfoc/gmsm/sm3"
)

/*************************************************************
*
*                normal
*************************************************************/
func Test_main(t *testing.T) {
	main()
	fmt.Println("end")
}

//org1 blocknum=4 org2 blocknum=4
func Test_pvtdataStoreRead(t *testing.T) {
	channel := "hxyz"
	packageID := "archive_1.0:7722f1bc17fbf3eb97b9d66c4fd8cc6ff4bf5872d57a81afe22fb7e5f527a675"
	dbpath := "/home/hxyz/Desktop/golang/src/network/gm-native/peer0.org1.example.com/hyperledger/production/ledgersData/pvtdataStore"
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", dbpath, err)
	}
	defer db.Close()
	datakey := &pvdataStore.DataKey{
		NsCollBlk: pvdataStore.NsCollBlk{
			Ns:     "_lifecycle",
			Coll:   "_implicit_org_Org1MSP",
			BlkNum: 3,
		},
		TxNum: 0,
	}
	skey := pvdataStore.EncodeDataKey(datakey)

	key := index.ConstructLevelKey(channel, skey)
	fmt.Println(string(key))
	value, err := db.Get(key, &opt.ReadOptions{})
	if err != nil {
		fmt.Println()
		panic(err)
	}
	fmt.Println("value", string(value), value)
	// fmt.Println(value[78:154])
	// fmt.Println([]byte("archive_1.0:cb2d1db4248398c0f567bb0787ea4cb7bea6ccf010b31e67df81d9f4af09c762"))
	var val []byte
	startB := value[:78]
	changeB := []byte(packageID)
	val = append(val, startB...)
	val = append(val, changeB...)
	val = append(val, value[154:]...)
	fmt.Println("nd", string(val), val)
	err = db.Put(key, val, &opt.WriteOptions{})
	if err != nil {
		fmt.Println()
		panic(err)
	}
	fmt.Println("value", string(val))
}

// sm3 hash key update
func Test_stateLevelDBSM3HashKey(t *testing.T) {
	channel := "hxyz"
	packageID := "archive_1.0:7722f1bc17fbf3eb97b9d66c4fd8cc6ff4bf5872d57a81afe22fb7e5f527a675"
	dbpath := "/home/hxyz/Desktop/golang/src/network/gm-native/peer0.org2.example.com/hyperledger/production/ledgersData/stateLeveldb"
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", dbpath, err)
	}
	defer db.Close()
	// build  sm3 hash key   parame modifity
	ChaincodeSourcesName := "chaincode-sources"
	privateName := "archive#1"
	coby := []byte(FieldKey(ChaincodeSourcesName, privateName, "PackageID"))
	seKey := stateLeveDB.EncodeDataKey("_lifecycle$$h_implicit_org_Org2MSP", string(sm3.Sm3Sum(coby)))
	Key := index.ConstructLevelKey(channel, seKey)
	value, err := db.Get(Key, &opt.ReadOptions{})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("value=", string(value))
	//change byte

	encodedCCHash := protoutil.MarshalOrPanic(&lb.StateData{
		Type: &lb.StateData_String_{String_: packageID},
	})
	// fmt.Println("y", sm3.Sm3Sum(encodedCCHash))
	// fmt.Println("bb\n", string(sm3.Sm3Sum(encodedCCHash)))

	var val []byte
	startB := value[:7]
	changeB := sm3.Sm3Sum(encodedCCHash)
	val = append(val, startB...)
	val = append(val, changeB...)
	fmt.Println("nd", string(val), val)
	err = db.Put(Key, val, &opt.WriteOptions{})
	if err != nil {
		panic(err)
	}
}

func Test_stateLevelDBLaws(t *testing.T) {
	channel := "hxyz"
	dbpath := "/home/hxyz/Desktop/golang/src/network/gm-native/peer0.org1.example.com/hyperledger/production/ledgersData/stateLeveldb"

	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", dbpath, err)
	}
	defer db.Close()

	// PACKAGEID Modifity
	ns := "_lifecycle$$p_implicit_org_Org1MSP"
	ckey := "chaincode-sources/fields/archive#1/PackageID"
	packageID := "archive_1.0:7722f1bc17fbf3eb97b9d66c4fd8cc6ff4bf5872d57a81afe22fb7e5f527a675"
	stateLeveDB.UpdateCCPackageID(db, channel, ns, ckey, packageID)

}

func Test_stateLevelDBBase(t *testing.T) {
	channel := "hxyz"
	srcPath := "/home/hxyz/Desktop/golang/src/network/gm-native/peer0.org2.example.com/hyperledger_bak/production/ledgersData/stateLeveldb"

	srcDB, err := leveldb.OpenFile(srcPath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", srcPath, err)
	}
	defer srcDB.Close()

	dstPath := "/home/hxyz/Desktop/golang/src/network/gm-native/peer0.org2.example.com/hyperledger/production/ledgersData/stateLeveldb"

	DstDB, err := leveldb.OpenFile(dstPath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", dstPath, err)
	}
	defer DstDB.Close()

	nsKeySep := []byte{0x00}
	sekey := append(append(stateLeveDB.DataKeyPrefix, nsKeySep...), []byte("CHANNEL_CONFIG_ENV_BYTES")...)
	Key := index.ConstructLevelKey(channel, sekey)
	value, err := srcDB.Get(Key, &opt.ReadOptions{})
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println("value=", string(value))
	//update dst ledger
	err = DstDB.Put(Key, value, &opt.WriteOptions{})
	if err != nil {
		panic(err)
	}
}

/******************************************************************************************
*              test
*
************************************************************************************************/
func readAll(db *leveldb.DB) {
	// var a bytes.Buffer
	// a.WriteString(channel)
	// a.WriteByte(0)
	// prefix_a := a.String()
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		//if strings.HasPrefix(key, prefix_a) {
		value := string(iter.Value())
		fmt.Printf("Key[%s]=[%s]\n", key, value)
		//}
	}
	iter.Release()
}

func Test_all(t *testing.T) {

	model := "readAll"
	//model := "readAllType"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/orderer.example.com/index"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/bookkeeper"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/chains/index"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/configHistory"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/fileLock"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/historyLeveldb"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/ledgerProvider"
	dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/pvtdataStore"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/ledgersData/stateLeveldb"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org1.example.com/transientstore"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/bookkeeper"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/chains/index"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/configHistory"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/fileLock"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/historyLeveldb"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/ledgerProvider"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/pvtdataStore"
	//dbpath := "/home/hxyz/Desktop/golang/src/network/gm-single/docker/peer0.org2.example.com/ledgersData/stateLeveldb"
	key := ""

	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", dbpath, err)
	}
	defer db.Close()

	switch model {
	// 获取一个账本所有k = v值
	case "readAll":
		readAll(db)

	/*
	* orderer账本索引
	* 账本路径 orderer.example.com/index
	 */
	// 获取orderer账本所有索引k=v值
	case "readOrdererAllIndex":
		oIndex.ReadOrdererAllIndex(db, channel)

	// 根据区块高度更新orderer账本索引
	case "updateOrdererIndexByBlockNum": //n
		//channel := "hxyz"
		channel := "system-channel"
		fileSuffixNum := 0
		offset := 18932
		bytesLength := 0
		var blockNum uint64 = 1
		oIndex.UpdateOrdererIndexByBlockNum(db, channel, blockNum, fileSuffixNum, offset, bytesLength)

	case "getAllBookkeeperKV":
		bookkeeper.ParseAllBookkeeperKV(db, "hxyz")
	/*
	* 节点账本索引
	* 账本路径 peer0.org1.example.com/ledgersData/chains/index
	 */
	// 获取所有索引k=v值
	case "readPeerAllIndex":
		pIndex.ReadPeerAllIndex(db, channel)
	// 	在索引中获取记录最后一个块
	case "lastBlockIndex":
		channel := "hxyz"
		_, err := pIndex.GetLastBlockIndexed(db, channel, []byte(key))
		if err != nil {
			fmt.Printf("get last blockNum error%s", err)
		}
		return
	// 根据区块高度获取 此区块索引和区块hash
	case "getBlockLockByNum": //n
		// second parame is block num 区块高度
		channel := "hxyz"
		peerSrcPath := ""
		pIndex.GetBlockLocByBlockNum(db, channel, peerSrcPath, 7)
	//	根据区块hash获取索引
	case "getBlockLockByHash": //h
		// second parame is block num 区块Hash
		//hash := index.GetBlockHashByBlockNum(db, 7)
		channel := "hxyz"
		hashS := "9784f53b55ab6bca245b6283d10108ec30de222f27ea063456bacbffbe440e81"
		hash, _ := hex.DecodeString(hashS)
		pIndex.GetBlockLocByHash(db, channel, hash)
	// 	根据交易txid获取索引
	case "getBlockLocByTxID": //t
		//指定区块获取区块内所有交易
		channel := "hxyz"
		txid := pIndex.GetBlockTxidByBlockNum(db, channel, 7)
		//txID := "8fd1815a371c848c5f17a6a644640ca01fac0bcda3ed4599ec76cd50a535033c"
		for _, txID := range txid {
			pIndex.GetBlockLocByTxID(db, channel, txID)
		}
	//	根据区块高度和交易在此区块高度获取索引
	case "getTXLocByBlockNumTranNum":
		//指定区块获取区块内所有交易
		channel := "hxyz"
		txid := pIndex.GetBlockTxidByBlockNum(db, channel, 3)
		//txID := "8fd1815a371c848c5f17a6a644640ca01fac0bcda3ed4599ec76cd50a535033c"
		for i, _ := range txid {
			pIndex.GetTXLocByBlockNumTranNum(db, channel, 3, uint64(i))
		}

	// 根据区块hash更新索引
	case "updateBlockByHash": //h
		channel := "hxyz"
		fileSuffixNum := 0
		offset := 95332
		bytesLength := 0
		hash, _ := hex.DecodeString("9784f53b55ab6bca245b6283d10108ec30de222f27ea063456bacbffbe440e81")
		pIndex.UpdateBlockByHash(db, channel, hash, fileSuffixNum, offset, bytesLength)
	// 	根据区块高度更新索引
	case "updateBlockLocByBlockNum": //n
		channel := "hxyz"
		fileSuffixNum := 0
		offset := 95332
		bytesLength := 0
		var blockNum uint64 = 7
		pIndex.UpdateBlockLocByBlockNum(db, channel, blockNum, fileSuffixNum, offset, bytesLength)
	// 	根据交易txid更新索引
	case "updateBlockAndTxLocByTxID": //t
		channel := "hxyz"
		var blockNum uint64 = 7
		txid := "8fd1815a371c848c5f17a6a644640ca01fac0bcda3ed4599ec76cd50a535033c"
		bfileSuffixNum := 0
		boffset := 95332
		bbytesLength := 0
		tfileSuffixNum := 0
		toffset := 95402
		tbytesLength := 5318
		pIndex.UpdateBlockAndTxLocByTxID(db, channel, blockNum, txid, bfileSuffixNum, boffset, bbytesLength, tfileSuffixNum, toffset, tbytesLength)
	// 根据区块高度和交易在此区块高度更新索引
	case "updateTxLocByBlockNumTranNum": //a
		channel := "hxyz"
		var blockNum uint64 = 7
		var tranNum uint64 = 0
		fileSuffixNum := 0
		offset := 95402
		bytesLength := 5318
		pIndex.UpdateTxLocByBlockNumTranNum(db, channel, blockNum, tranNum, fileSuffixNum, offset, bytesLength)
	// 更新索引中最后一个块高度
	case "updateLastBlockIndex":
		var blockNum uint64 = 7
		channel := "hxyz"
		//channel := "system-channel"
		err := index.UpdateLastBlockIndex(db, channel, blockNum)
		if err != nil {
			fmt.Printf("UpdateLastBlockIndex error%s", err)
		}
		return
	// 	更新索引中区块管理信息
	case "updateBlkMgrInfo":
		//channel := "hxyz"
		channel := "system-channel"
		var lastPersistedBlock uint64 = 1
		latestFileNumber := 0
		latestFileSize := 44117
		NoBlockFiles := false
		err := index.UpdateBlkMgrInfo(db, channel, lastPersistedBlock, latestFileNumber, latestFileSize, NoBlockFiles)
		if err != nil {
			fmt.Printf("UpdateBlkMgrInfo error%s", err)
		}
		return
	/*
	* 历史配置账本
	* 账本路径 peer0.org1.example.com/ledgersData/configHistory
	 */
	// 获取账本所有k-v值，并解析key和value成字符串显示
	case "getallConfigHistorykv":
		channel := "hxyz"
		chaincode := "archive"
		pHistory.ParseConfigHistoryKV(db, channel, chaincode)
	// 根据区块高度获取历史配置值 并解析value成字符串显示
	case "getPeerConfigHistory":
		channel := "hxyz"
		chaincode := "archive"
		var blockNum uint64 = 5
		pHistory.GetPeerConfigHistory(db, channel, chaincode, blockNum)

	// 更新历史配置信息
	case "updateConfigHistory": //注意其中一部分参数需要改动内部逻辑固定变量，这些变量需要根据变动改变
		var blockNum uint64 = 5
		channel := "hxyz"
		chaincode := "archive"
		pHistory.UpdateConfigHistory(db, channel, chaincode, blockNum)

	/*
	* 历史账本数据
	* 账本路径 peer0.org1.example.com/ledgersData/historyLeveldb
	 */
	// 获取历史账本数据所有k-v值
	case "getAllHistoryLedgerKV":
		channel := "hxyz"
		historyledger.GetAllHistoryLedgerKV(db, channel)
	// 	更新历史账本数据
	case "updateHistoryLedger":
		channel := "hxyz"
		ns := "s"
		rwsetKey := ""
		var blockNo uint64 = 5
		var tranNo uint64 = 1
		historyledger.UpdateHistoryLedger(db, channel, ns, rwsetKey, blockNo, tranNo)

	/*
	* 账本提供者
	* 账本路径 peer0.org1.example.com/ledgersData/ledgerProvider
	 */
	//获取账本所有k-v值
	case "getAllLedgerProviderKV":
		channel := "hxyz"
		ledgerProvider.ParseAllLedgerProviderKV(db, channel)
	//更新账本状态
	case "updateLedgerProviderStatus":
		channel := "hxyz"
		ledgerProvider.UpdateLedgerProviderBlock(db, channel)

	/*
	* 私有数据存储
	* 账本路径 peer0.org1.example.com/ledgersData/pvtdataStore
	 */
	//
	case "getAllPvdataStoreKV":
		channel := "hxyz"
		pvdataStore.ParseAllPvdataStoreKV(db, channel)

	/*
	* state
	 */
	case "getAllStateLeveldbKV":
		channel := "hxyz"
		stateLeveDB.ParseAllStateLevelDBKV(db, channel)
	case "getPackIDInfoByKey":
		channel := "hxyz"
		stateLeveDB.GetPackIDInfoByKey(db, channel)
	case "updateCCPackageID":
		channel := "hxyz"
		ns := "_lifecycle$$p_implicit_org_Org1MSP"
		ckey := "chaincode-sources/fields/archive#1/PackageID"
		packageID := "archive_1.0:cb2d1db4248398c0f567bb0787ea4cb7bea6ccf010b31e67df81d9f4af09c762"
		stateLeveDB.UpdateCCPackageID(db, channel, ns, ckey, packageID)
	}
}

const (
	MetadataInfix = "metadata"
	FieldsInfix   = "fields"
)

func Test_StringHash(t *testing.T) {
	encodedCCHash := protoutil.MarshalOrPanic(&lb.StateData{
		Type: &lb.StateData_String_{String_: "archive_1.0:428614508a088a9a433385ff2080da86e4680b0cfe55bcbd5b42cdea8061a6c0"},
	})
	fmt.Println("y", sm3.Sm3Sum(encodedCCHash))
	fmt.Println("bb\n", string(sm3.Sm3Sum(encodedCCHash)))
	fmt.Println("ss", string(encodedCCHash))
	// ChaincodeSourcesName := "chaincode-sources"
	// privateName := "archive#1"
	// coby := []byte(FieldKey(ChaincodeSourcesName, privateName, "PackageID"))
	// fmt.Println(string(sm3.Sm3Sum(coby)))
}
func FieldKey(namespace, name, field string) string {
	return fmt.Sprintf("%s/%s/%s/%s", namespace, FieldsInfix, name, field)
}
