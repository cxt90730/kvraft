package kvraft

import (
	"fmt"
)

const (
	CmdGet          = "GET"
	CmdSet          = "SET"
	CmdDel          = "DEL"
	CmdShare        = "SHARE"
	CmdJoin         = "JOIN"
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

func (s *KVRaftService) GetValue(bucketName, key []byte) ([]byte, error) {
	rpcCmd := RpcCmd{
		Op:     CmdGet,
		Key:    key,
		Bucket: bucketName,
	}

	return doGet(rpcCmd, s.fsm)
}

func (s *KVRaftService) SetValue(bucketName, key, value []byte) error {
	rpcCmd := RpcCmd{
		Op:     CmdSet,
		Key:    key,
		Bucket: bucketName,
		Value:  value,
	}

	rpcAddr := fmt.Sprintf("%s:%s", ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort)
	s.log.Println("[SetValue RPC] rpcAddr:", rpcAddr)
	return doSet(rpcCmd, rpcAddr)
}

func (s *KVRaftService) CreateBucket(bucketName []byte) error {
	rpcCmd := RpcCmd{
		Op:     CmdCreateBucket,
		Bucket: bucketName,
	}
	rpcAddr := fmt.Sprintf("%s:%s", ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort)
	s.log.Println("[Create Bucket RPC] rpcAddr:", rpcAddr)
	return doSet(rpcCmd, rpcAddr)
}

// AddPeer is used to add a new peer into the cluster. This must be
// run on the leader or it will fail.
func (s *KVRaftService) AddPeer(peer string) {
	rpcCmd := RpcCmd{
		Op:    CmdJoin,
		Value: []byte(peer),
	}
	rpcAddr := fmt.Sprintf("%s:%s", ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort)
	s.log.Println("[Join Raft RPC] rpcAddr:", rpcAddr)
	doJoin(rpcCmd, rpcAddr)

}

func doJoin(rc RpcCmd, rpcAddr string) error {
	opRequest := OpRequest{
		Op:    rc.Op,
		Value: rc.Value,
	}
	return doRpcRequest(rpcAddr, &opRequest)
}

func doGet(rc RpcCmd, fsm *RaftFSM) ([]byte, error) {
	return fsm.Get(rc.Bucket, rc.Key)
}

func doSet(rc RpcCmd, rpcAddr string) error {
	opRequest := OpRequest{
		Op:     rc.Op,
		Bucket: string(rc.Bucket),
		Key:    string(rc.Key),
		Value:  rc.Value,
	}
	return doRpcRequest(rpcAddr, &opRequest)
}

func (rc RpcCmd) DoDel() {
	return
}

func doRpcRequest(rpcAddr string, opRequest *OpRequest) error {
	reply, err := newRpcClient().RPCRequest(rpcAddr, opRequest)
	if err != nil || reply.Status == RPC_STATUS_FAIL {
		err = fmt.Errorf("%s", reply.Msg)
	}
	return err
}
