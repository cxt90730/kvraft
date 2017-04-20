package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type ServerConfig struct {
	ServerAddr       string
	ServerPort       string
	RaftAddr         string
	RaftPort         string
	RpcAddr          string
	RpcPort          string
	LeaderAddr       string
	LeaderPort       string
	PeerStorage      string
	Peers            []string
	SnapshotStorage  string
	DbPath           string
	LogDir           string
	EnableSingleNode bool
	BucketName       string
}

func (sc *ServerConfig) RaftAddrString() string {
	return fmt.Sprintf("%s:%s", sc.RaftAddr, sc.RaftPort)
}

func (sc *ServerConfig) RpcAddrString() string {
	return fmt.Sprintf("%s:%s", sc.RpcAddr, sc.RpcPort)
}

//Conf content must be formatted by json

var serverConfig *ServerConfig

func NewServerConfig(confPath string) (*ServerConfig, error) {
	serverConfig = &ServerConfig{}
	cFile, err := os.Open(confPath)
	if err != nil {
		return nil, err
	}
	defer cFile.Close()
	err = json.NewDecoder(cFile).Decode(serverConfig)
	if err != nil {
		return nil, err
	}
	return serverConfig, nil
}
