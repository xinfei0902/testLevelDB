package config

import (
	"fmt"

	"github.com/go-ini/ini"
)

var (
	Conf *Config
)

type Config struct {
	Path LedgerPath
}

type LedgerPath struct {
	OrdererSrcPath []string
	OrdererDstPath []string
	PeerSrcPath    []string
	PeerDstPath    []string
	Channel        string
}

func init() {
	Conf = &Config{}
}

func Init(confPath string) error {
	if err := ini.MapTo(Conf, confPath); err != nil {
		return err
	}
	fmt.Println("init config end")
	return nil
}
