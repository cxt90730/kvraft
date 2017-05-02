package kvraft

import (
	"encoding/json"
	"fmt"
	"os"
)

type KVRaftConfig struct {
	RaftAddr         string
	RaftPort         string
	RpcAddr          string
	RpcPort          string
	LeaderRpcAddr    string
	LeaderRpcPort    string
	MemberName       string // {ip}:{port}
	MemberAddr       string
	MemberPort       int
	PeerStorage      string
	Peers            []string
	SnapshotStorage  string
	DbDir            string
	EnableSingleNode bool
	RaftLogDir       string
	BucketName       string
}

func (rc *KVRaftConfig) RaftAddrString() string {
	return fmt.Sprintf("%s:%s", rc.RaftAddr, rc.RaftPort)
}

func (rc *KVRaftConfig) RpcAddrString() string {
	return fmt.Sprintf("%s:%s", rc.RpcAddr, rc.RpcPort)
}

func (rc *KVRaftConfig) MemberAddrString() string {
	return fmt.Sprintf("%s:%s", rc.MemberAddr, rc.MemberPort)
}

func NewKVRaftConfig(confPath string) (*KVRaftConfig, error) {
	kvraftConfig := &KVRaftConfig{}
	cFile, err := os.Open(confPath)
	if err != nil {
		return nil, err
	}
	defer cFile.Close()
	err = json.NewDecoder(cFile).Decode(kvraftConfig)
	if err != nil {
		return nil, err
	}
	return kvraftConfig, nil
}
