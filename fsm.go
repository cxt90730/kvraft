package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

//TODO:
//###Snapshot, Restore

var ErrNotFound = fmt.Errorf("the key's value is nil.")
var ErrInvalidCmd = fmt.Errorf("the command is invalid")

// RaftFSM is an implementation of the FSM interfaces
// It just store the key/value logs sequentially
type RaftFSM struct {
	mu *sync.Mutex
	rs StorageDB
}

func NewStorageFSM(rs StorageDB) *RaftFSM {
	return &RaftFSM{
		mu: &sync.Mutex{},
		rs: rs,
	}
}

// Apply is only called by leader in cluster
// the format of log must be json
// {"cmd":op, "key":key, "value": value}
// TODO
// use protocol buffer instead of json format
func (fsm *RaftFSM) Apply(log *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	var req OpRequest
	var err error
	if err = json.Unmarshal(log.Data, &req); err != nil {
		return fmt.Errorf("Cannot unmarshal raft log: %s", string(log.Data))
	}

	switch req.Op {
	case CmdSet:
		err = fsm.rs.SetValue([]byte(req.Bucket), []byte(req.Key), req.Value)
	case CmdDel:
		err = fsm.rs.DeleteValue([]byte(req.Bucket), []byte(req.Key))
	case CmdShare:
		err = json.Unmarshal(req.Value, ShCache)
	default:
		err = ErrInvalidCmd
	}
	if err != nil {
		ServerLogger.Println("FSM Apply Error: ", err)
	}
	return err
}

type RaftFSMSnapshot struct {
	SnapShotStore StorageDB
}

func (fsm *RaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	ServerLogger.Println("Execute StorageFSM SnapShot")
	return &RaftFSMSnapshot{
		SnapShotStore: fsm.rs,
	}, nil
}

func (fsm *RaftFSM) Restore(irc io.ReadCloser) error {
	return nil
}

func (snap *RaftFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (snap *RaftFSMSnapshot) Release() {
	return
}

// Get a value by bucketName and key
func (fsm *RaftFSM) Get(bucket, key []byte) ([]byte, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	value, err := fsm.rs.GetValue(bucket, key)
	if err != nil {
		return nil, err
	}
	return value, nil
}
