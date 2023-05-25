package process

import (
	"fmt"
	"io"
	"os"
	"testLevelDB/config"
	"testLevelDB/index"
	oIndex "testLevelDB/index/orderer"
	pIndex "testLevelDB/index/peer"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func LedgerProcess() error {
	// 开始处理orderer块文件
	err := ProcessOrdererBlockFile()
	if err != nil {
		return err
	}
	// 开始处理orderer索引
	err = ProcessOrdererIndex()
	if err != nil {
		return err
	}
	//开始处理peer块文件
	err = ProcessPeerBlockFile()
	if err != nil {
		return err
	}
	// peer索引处理
	err = ProcessPeerIndex()
	if err != nil {
		return err
	}
	fmt.Println("处理orderer和peer块文件、索引完毕")
	return nil
}

func ProcessOrdererIndex() error {
	// 1. get
	ordererSrcPath := config.Conf.Path.OrdererSrcPath
	ordererDstPath := config.Conf.Path.OrdererDstPath
	channel := config.Conf.Path.Channel
	for o := 0; o < len(ordererSrcPath); o++ {
		ordererIndexSrcPath := fmt.Sprintf("%s/index", ordererSrcPath[o])
		ordererIndexDstPath := fmt.Sprintf("%s/index", ordererDstPath[o])
		//打开docker下orderer索引
		db, err := leveldb.OpenFile(ordererIndexSrcPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", ordererIndexSrcPath, err)
		}
		defer db.Close()
		//打开native下orderer索引
		dstdb, err := leveldb.OpenFile(ordererIndexDstPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", ordererIndexDstPath, err)
		}
		defer dstdb.Close()
		// 从docker索引中获取 记录最后一个区块高度
		latestblockNum, err := pIndex.GetLastBlockIndexed(db, channel, nil)
		if err != nil {
			fmt.Printf("get last blockNum error%s", err)
		}
		// 更新native里的orderer索引 记录最后一个区块高度
		err = index.UpdateLastBlockIndex(dstdb, channel, latestblockNum)
		if err != nil {
			fmt.Printf("UpdateLastBlockIndex error%s", err)
		}
		// 获取native 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		dstblkMgrInfo, err := oIndex.GetBlockMgrInfo(dstdb, channel)
		if err != nil {
			return err
		}
		// 获取docker 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		srcblkMgrInfo, err := oIndex.GetBlockMgrInfo(db, channel)
		if err != nil {
			return err
		}
		dstLastFileSize := dstblkMgrInfo.LatestFileSize
		//获取第六个块的偏移量、所在区块文件（blockfile_***）后缀标识 、字节长度等
		blockLock, err := oIndex.GetBlockLocByBlockNum(db, channel, uint64(6))
		if err != nil {
			return err
		}
		blockOffset := blockLock.LocPointer.Offset
		/*比较docker第六个区块开始偏移量和native块文件最后偏移量，相差多少，以便于往native写时每一个区块开始位置能够找到对应开始位置
		*比如相差五个字节，那么从docker读取第六个区块往native写的时候，
		*因为相差所有往native追加位置相应（加减五个字节）
		 */
		var differSize, flage int //flage  docker第六个区块开始偏移量比native最后记录偏移量大则flage=2 否则flage=1
		if dstLastFileSize > blockOffset {
			flage = 1
			differSize = dstLastFileSize - blockOffset
		} else {
			differSize = blockOffset - dstLastFileSize
			flage = 2
		}
		// 从第六个区块开始，更新native的orderer索引
		for b := 6; b <= int(latestblockNum); b++ {
			// 从docker读写相应区块高度区块索引
			srcblockLock, err := oIndex.GetBlockLocByBlockNum(db, channel, uint64(b))
			if err != nil {
				return err
			}
			// 把读取信息，进行偏移修改，更新到native中
			blockNum := uint64(b)
			fileSuffixNum := srcblockLock.FileSuffixNum
			offset := 0
			if flage == 1 {
				offset = srcblockLock.Offset + differSize
			} else {
				offset = srcblockLock.Offset - differSize
			}
			bytesLength := srcblockLock.BytesLength
			err = oIndex.UpdateOrdererIndexByBlockNum(dstdb, channel, blockNum, fileSuffixNum, offset, bytesLength)
			if err != nil {
				return nil
			}

		}
		//更新native 记录最新区块高度、块文件(blockfile_**)的后缀、最后块文件最后记录信息偏移位置(字节末尾)
		lastPersistedBlock := srcblkMgrInfo.LastPersistedBlock
		latestFileNumber := srcblkMgrInfo.LatestFileNumber
		latestFileSize := 0
		if flage == 1 {
			latestFileSize = srcblkMgrInfo.LatestFileSize + differSize
		} else {
			latestFileSize = srcblkMgrInfo.LatestFileSize - differSize
		}
		NoBlockFiles := srcblkMgrInfo.NoBlockFiles
		err = index.UpdateBlkMgrInfo(dstdb, channel, lastPersistedBlock, latestFileNumber, latestFileSize, NoBlockFiles)
		if err != nil {
			fmt.Printf("UpdateBlkMgrInfo error%s", err)
		}

	}

	return nil
}

func ProcessOrdererBlockFile() error {
	//
	ordererSrcPath := config.Conf.Path.OrdererSrcPath
	ordererDstPath := config.Conf.Path.OrdererDstPath
	channel := config.Conf.Path.Channel
	for o := 0; o < len(ordererSrcPath); o++ {
		ordererIndexSrcPath := fmt.Sprintf("%s/index", ordererSrcPath[o])
		ordererIndexDstPath := fmt.Sprintf("%s/index", ordererDstPath[o])
		db, err := leveldb.OpenFile(ordererIndexSrcPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", ordererIndexSrcPath, err)
			panic(err)
		}
		defer db.Close()
		//获取第六个块的偏移量、所在区块文件（blockfile_***）后缀标识 、字节长度等
		blockLock, err := oIndex.GetBlockLocByBlockNum(db, channel, uint64(6))
		if err != nil {
			return err
		}
		//根据获取文件后缀，组装文件路径
		blockFileName := index.BlockfilePrefix + fmt.Sprintf("%06d", blockLock.FileSuffixNum)
		ordererBlockFileSrcPath := fmt.Sprintf("%s/chains/%s/%s", ordererSrcPath[o], channel, blockFileName)
		ordererBlockFileDstPath := fmt.Sprintf("%s/chains/%s/%s", ordererDstPath[o], channel, blockFileName)
		//获得原生（docker）记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		srcblkMgrInfo, err := oIndex.GetBlockMgrInfo(db, channel)
		if err != nil {
			return err
		}
		//从blockfile_*** 中第六个块起始偏移量开始读取
		var file *os.File
		if file, err = os.OpenFile(ordererBlockFileSrcPath, os.O_RDONLY, 0600); err != nil {
			return errors.Wrapf(err, "error opening block file %s", ordererBlockFileSrcPath)
		}

		// 设置seek位置 是第六个块开始位置
		if newPosition, err := file.Seek(int64(blockLock.Offset), 0); err != nil {
			return errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", ordererBlockFileSrcPath, newPosition)
		}
		// 此处是测试只有一个blockfiel_***，因此用最后文件大小减去第六个块起始，计算所有字节大小，如果是多个blockfile_**就不能这样用了
		byLen := srcblkMgrInfo.LatestFileSize - blockLock.Offset
		buff := make([]byte, byLen)
		n, err := file.Read(buff)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		// 打开native orderer块文件
		dstdb, err := leveldb.OpenFile(ordererIndexDstPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", ordererIndexDstPath, err)
		}
		defer dstdb.Close()
		//获取native的最后 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		dstblkMgrInfo, err := oIndex.GetBlockMgrInfo(dstdb, channel)
		if err != nil {
			return err
		}
		var wfile *os.File
		if wfile, err = os.OpenFile(ordererBlockFileDstPath, os.O_RDWR, 0660); err != nil {
			return errors.Wrapf(err, "error opening block file %s", ordererBlockFileDstPath)
		}
		//设置seek 在native的第五区块末尾开始追加docker第六个区块开始内容
		var newPosition int64
		if newPosition, err = wfile.Seek(int64(dstblkMgrInfo.LatestFileSize), 0); err != nil {
			return errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", ordererBlockFileSrcPath, newPosition)
		}

		_, err = wfile.Write(buff)
		if err != nil && err != io.EOF {
			panic(err)
		}

	}

	return nil
}

func ProcessPeerIndex() error {
	peerSrcPath := config.Conf.Path.PeerSrcPath
	peerDstPath := config.Conf.Path.PeerDstPath
	channel := config.Conf.Path.Channel
	for o := 0; o < len(peerSrcPath); o++ {
		peerIndexSrcPath := fmt.Sprintf("%s/index", peerSrcPath[o])
		peerIndexDstPath := fmt.Sprintf("%s/index", peerDstPath[o])
		//打开docker下orderer索引
		db, err := leveldb.OpenFile(peerIndexSrcPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", peerIndexSrcPath, err)
			panic(err)
		}
		defer db.Close()
		//打开native下orderer索引
		dstdb, err := leveldb.OpenFile(peerIndexDstPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", peerIndexDstPath, err)
			panic(err)
		}
		defer dstdb.Close()
		// 从docker索引中获取 记录最后一个区块高度
		latestblockNum, err := pIndex.GetLastBlockIndexed(db, channel, nil)
		if err != nil {
			fmt.Printf("get last blockNum error%s", err)
			return err
		}
		//
		// 更新native里的orderer索引 记录最后一个区块高度
		err = index.UpdateLastBlockIndex(dstdb, channel, latestblockNum)
		if err != nil {
			fmt.Printf("UpdateLastBlockIndex error%s", err)
			return err
		}
		// 获取native 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		dstblkMgrInfo, err := oIndex.GetBlockMgrInfo(dstdb, channel)
		if err != nil {
			return err
		}
		// 获取docker 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		srcblkMgrInfo, err := oIndex.GetBlockMgrInfo(db, channel)
		if err != nil {
			return err
		}
		dstLastFileSize := dstblkMgrInfo.LatestFileSize
		//获取第六个块的偏移量、所在区块文件（blockfile_***）后缀标识 、字节长度等
		blockLock, err := oIndex.GetBlockLocByBlockNum(db, channel, uint64(6))
		if err != nil {
			return err
		}
		blockOffset := blockLock.LocPointer.Offset
		/*比较docker第六个区块开始偏移量和native块文件最后偏移量，相差多少，以便于往native写时每一个区块开始位置能够找到对应开始位置
		*比如相差五个字节，那么从docker读取第六个区块往native写的时候，
		*因为相差所有往native追加位置相应（加减五个字节）
		 */
		var differSize, flage int
		if dstLastFileSize > blockOffset {
			flage = 1
			differSize = dstLastFileSize - blockOffset
		} else {
			differSize = blockOffset - dstLastFileSize
			flage = 2
		}
		/* 从第六个区块开始，从四个方向，更新native的orderer索引
		*   a: 通过区块高度和交易在此区块高度组成key
		*   t: 通过交易(txid)组织成key更新
		*   h: 通过区块hash组成key更新
		*   n: 通过区块高度组成key更新
		 */
		for b := 6; b <= int(latestblockNum); b++ {
			blockNum := uint64(b)
			// 通过区块高度，获取区块内所有交易txid
			txid := pIndex.GetBlockTxidByBlockNum(db, channel, blockNum)
			//遍历交易 更新每条交易 所在块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
			for i, tx := range txid {
				//从docker处，根据区块高度和交易高度获取当前key存储信息
				txBlockInfo, err := pIndex.GetTXLocByBlockNumTranNum(db, channel, blockNum, uint64(i))
				if err != nil {
					return err
				}
				tranNum := uint64(i)
				fileSuffixNum := txBlockInfo.FileSuffixNum
				offset := 0
				if flage == 1 {
					offset = txBlockInfo.Offset + differSize
				} else {
					offset = txBlockInfo.Offset - differSize
				}
				bytesLength := txBlockInfo.BytesLength
				//更新native处，根据区块高度和交易高度组成key,更新记录块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
				err = pIndex.UpdateTxLocByBlockNumTranNum(dstdb, channel, blockNum, tranNum, fileSuffixNum, offset, bytesLength)
				if err != nil {
					return err
				}
				// 从docker处，根据txid获取存储块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
				blkFlp, txFlp, err := pIndex.GetBlockLocByTxID(db, channel, tx)
				if err != nil {
					return err
				}

				boffset := 0
				if flage == 1 {
					boffset = blkFlp.Offset + differSize
				} else {
					boffset = blkFlp.Offset - differSize
				}
				toffset := 0
				if flage == 1 {
					toffset = txFlp.Offset + differSize
				} else {
					toffset = txFlp.Offset - differSize
				}
				// 更新native处，根据txid组成key,更新记录块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
				err = pIndex.UpdateBlockAndTxLocByTxID(dstdb, channel, blockNum, tx, blkFlp.FileSuffixNum, boffset, blkFlp.BytesLength, txFlp.FileSuffixNum, toffset, txFlp.BytesLength)
				if err != nil {
					return err
				}
			}

			//从docker中 通过区块高度，获取区块hash
			hash := pIndex.GetBlockHashByBlockNum(db, channel, peerSrcPath[o], blockNum)
			// 从docker中 通过区块hash获取存储信息（记录块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度）
			blockHash, err := pIndex.GetBlockLocByHash(db, channel, hash)
			if err != nil {
				return err
			}
			fileSuffixNum := blockHash.FileSuffixNum
			offset := 0
			if flage == 1 {
				offset = blockHash.Offset + differSize
			} else {
				offset = blockHash.Offset - differSize
			}
			bytesLength := blockHash.BytesLength
			// 更新native处，根据区块hash组成key,更新记录块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
			err = pIndex.UpdateBlockByHash(dstdb, channel, hash, fileSuffixNum, offset, bytesLength)
			if err != nil {
				return err
			}

			//获取第某个块的偏移量、所在区块文件（blockfile_***）后缀标识 、字节长度等
			blockNumF, err := pIndex.GetBlockLocByBlockNum(db, channel, peerSrcPath[o], blockNum)
			if err != nil {
				return err
			}
			if flage == 1 {
				offset = blockNumF.Offset + differSize
			} else {
				offset = blockNumF.Offset - differSize
			}
			// 更新native处，根据区块高度组成key,更新记录块文件(blockfile_**)后缀、交易信息起始偏移量、字节长度
			err = pIndex.UpdateBlockLocByBlockNum(dstdb, channel, blockNum, blockNumF.FileSuffixNum, offset, blockNumF.BytesLength)
			if err != nil {
				return err
			}

		}
		//更新native 记录最新区块高度、块文件(blockfile_**)的后缀、最后块文件最后记录信息偏移位置(字节末尾)
		lastPersistedBlock := srcblkMgrInfo.LastPersistedBlock
		latestFileNumber := srcblkMgrInfo.LatestFileNumber
		latestFileSize := 0
		if flage == 1 {
			latestFileSize = srcblkMgrInfo.LatestFileSize + differSize
		} else {
			latestFileSize = srcblkMgrInfo.LatestFileSize - differSize
		}
		NoBlockFiles := srcblkMgrInfo.NoBlockFiles
		err = index.UpdateBlkMgrInfo(dstdb, channel, lastPersistedBlock, latestFileNumber, latestFileSize, NoBlockFiles)
		if err != nil {
			fmt.Printf("UpdateBlkMgrInfo error%s", err)
			return err
		}

	}

	return nil
}

func ProcessPeerBlockFile() error {
	//
	peerSrcPath := config.Conf.Path.PeerSrcPath
	peerDstPath := config.Conf.Path.PeerDstPath
	channel := config.Conf.Path.Channel
	for o := 0; o < len(peerSrcPath); o++ {
		peerIndexSrcPath := fmt.Sprintf("%s/index", peerSrcPath[o])
		peerIndexDstPath := fmt.Sprintf("%s/index", peerDstPath[o])
		db, err := leveldb.OpenFile(peerIndexSrcPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", peerIndexSrcPath, err)
		}
		defer db.Close()
		//获取第六个块的偏移量、所在区块文件（blockfile_***）后缀标识 、字节长度等
		blockLock, err := oIndex.GetBlockLocByBlockNum(db, channel, uint64(6))
		if err != nil {
			return err
		}
		//根据获取文件后缀，组装块文件路径
		blockFileName := index.BlockfilePrefix + fmt.Sprintf("%06d", blockLock.FileSuffixNum)
		peerBlockFileSrcPath := fmt.Sprintf("%s/chains/%s/%s", peerSrcPath[o], channel, blockFileName)
		peerBlockFileDstPath := fmt.Sprintf("%s/chains/%s/%s", peerDstPath[o], channel, blockFileName)
		//获得原生（docker）记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		srcblkMgrInfo, err := oIndex.GetBlockMgrInfo(db, channel)
		if err != nil {
			return err
		}
		//从blockfile_*** 中第六个块起始偏移量开始读取
		var file *os.File
		if file, err = os.OpenFile(peerBlockFileSrcPath, os.O_RDONLY, 0600); err != nil {
			return errors.Wrapf(err, "error opening block file %s", peerBlockFileSrcPath)
		}

		// 设置seek位置 是第六个块开始位置
		if newPosition, err := file.Seek(int64(blockLock.Offset), 0); err != nil {
			return errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", peerBlockFileSrcPath, newPosition)
		}
		// 此处是测试只有一个blockfiel_***，因此用最后文件大小减去第六个块起始，计算所有字节大小，如果是多个blockfile_**就不能这样用了
		byLen := srcblkMgrInfo.LatestFileSize - blockLock.Offset
		buff := make([]byte, byLen)
		n, err := file.Read(buff)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}
		// 打开native orderer块文件
		dstdb, err := leveldb.OpenFile(peerIndexDstPath, nil)
		if err != nil {
			fmt.Printf("ERROR: Cannot open LevelDB from [%s], with error=[%v]\n", peerBlockFileDstPath, err)
		}
		defer dstdb.Close()
		//获取native的最后 记录最新区块信息 包含最后区块高度、最后块文件后缀、最后块文件字节偏移量（也就是整个文件字节长度）
		dstblkMgrInfo, err := oIndex.GetBlockMgrInfo(dstdb, channel)
		if err != nil {
			return err
		}
		var wfile *os.File
		if wfile, err = os.OpenFile(peerBlockFileDstPath, os.O_RDWR, 0660); err != nil {
			return errors.Wrapf(err, "error opening block file %s", peerBlockFileDstPath)
		}
		//设置seek 在native的第五区块末尾开始追加docker第六个区块开始内容
		var newPosition int64
		if newPosition, err = wfile.Seek(int64(dstblkMgrInfo.LatestFileSize), 0); err != nil {
			return errors.Wrapf(err, "error seeking block file [%s] to startOffset [%d]", peerBlockFileDstPath, newPosition)
		}

		_, err = wfile.Write(buff)
		if err != nil && err != io.EOF {
			panic(err)
		}

	}

	return nil
}
