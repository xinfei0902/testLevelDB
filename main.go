package main

import (
	"os"
	"os/signal"
	"syscall"
	"testLevelDB/config"
	"testLevelDB/process"
)

var (
	channel = "hxyz"
)

func main() {
	if err := config.Init("./config.ini"); err != nil {
		panic(err)
	}

	// if err := config.Init(os.Args[1]); err != nil {
	// 	panic(err)
	// }
	/*
	* 此函数处理了orderer和peer块文件和索引文件
	 */
	err := process.LedgerProcess()
	if err != nil {
		panic(err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			return
		case syscall.SIGHUP:
			// TODO reload
		default:
			return
		}
	}
}
