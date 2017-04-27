package main

import ()
import (
	"fmt"
)

const (
	CmdGet          = "GET"
	CmdSet          = "SET"
	CmdDel          = "DEL"
	CmdShare        = "SHARE"
	CmdGetBucket    = "GET BUCKET"
	CmdCreateBucket = "CREATE BUCKET"
	CmdDelBucket    = "DEL BUCKET"
)

const (
	QsConsistent = iota
	QsRandom
)

type RpcCmd struct {
	Op     string
	Bucket []byte
	Key    []byte
	Value  []byte
}

func (rc RpcCmd) DoGet() ([]byte, error) {
	return localServer.FSM.Get(rc.Bucket, rc.Key)
}

func (rc RpcCmd) DoSet() error {
	rpcAddr := fmt.Sprintf("%s:%s", serverConfig.LeaderRpcAddr, serverConfig.LeaderRpcPort)
	opRequest := OpRequest{
		Op:     rc.Op,
		Bucket: string(rc.Bucket),
		Key:    string(rc.Key),
		Value:  rc.Value,
	}
	reply, err := NewRpcClient().RPCRequest(rpcAddr, &opRequest)
	if err != nil || reply.Status == RPC_STATUS_FAIL {
		err = fmt.Errorf("%s", reply.Msg)
	}
	return err
}

func (rc RpcCmd) DoDel() {
	return
}
