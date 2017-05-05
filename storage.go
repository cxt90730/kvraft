package kvraft

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"sync"
)

var (
	STABLE_STORE_BUCKET = []byte("stable_store")
	LOG_STORE_BUCKET    = []byte("log_store")
)

//Implements StableStore interface and LogStore interface

// ================ StableStore Interface ================ //

type RaftStorage struct {
	rdb *RaftDB
	//used by log store
	mu *sync.Mutex
}

func (rs *RaftStorage) Set(key []byte, val []byte) error {
	return rs.rdb.SetValue(STABLE_STORE_BUCKET, key, val)
}

func (rs *RaftStorage) Get(key []byte) ([]byte, error) {
	return rs.rdb.GetValue(STABLE_STORE_BUCKET, key)
}

// SetUint64 implements the StableStore interface.
func (rs *RaftStorage) SetUint64(key []byte, val uint64) error {
	return rs.rdb.SetValue(STABLE_STORE_BUCKET, key, Itob(val))
}

// GetUint64 implements the StableStore interface.
func (rs *RaftStorage) GetUint64(key []byte) (uint64, error) {
	valByte, err := rs.rdb.GetValue(STABLE_STORE_BUCKET, key)
	if err != nil {
		return 0, err
	}
	//Empty value
	if valByte == nil {
		return 0, nil
	}
	return Btoi(valByte), nil
}

// ================================================================= //

// ================ LogStore Interface ================ //

// FirstIndex returns the first index written. 0 for no entries.
func (rs *RaftStorage) FirstIndex() (uint64, error) {
	index, _, err := rs.rdb.GetFirst(LOG_STORE_BUCKET)
	if err != nil || index == nil {
		return 0, err
	}
	return Btoi(index), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (rs *RaftStorage) LastIndex() (uint64, error) {
	index, _, err := rs.rdb.GetLast(LOG_STORE_BUCKET)
	if err != nil || index == nil {
		return 0, err
	}
	return Btoi(index), nil
}

// GetLog gets a log entry at a given index.
// raft.Log  must be json format
func (rs *RaftStorage) GetLog(index uint64, log *raft.Log) error {
	val, err := rs.rdb.GetValue(LOG_STORE_BUCKET, Itob(index))
	if err != nil {
		return err
	}
	err = json.Unmarshal(val, log)
	if err != nil {
		return err
	}
	return nil
}

// StoreLog stores a log entry.
func (rs *RaftStorage) StoreLog(log *raft.Log) error {
	return rs.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries.
func (rs *RaftStorage) StoreLogs(logs []*raft.Log) error {
	err := rs.rdb.DoBatch(LOG_STORE_BUCKET,
		func(tx *bolt.Tx, bucketName []byte) error{
			b := tx.Bucket(bucketName)
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			for _, log := range logs {
				key := Itob(log.Index)
				val, err := json.Marshal(log)
				if err != nil {
					return err
				}
				if err = b.Put(key, val); err != nil {
					return err
				}
			}
			return nil
		})
	return err
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (rs *RaftStorage) DeleteRange(min, max uint64) error {
	err := rs.rdb.DoBatch(LOG_STORE_BUCKET,
		func(tx *bolt.Tx, bucketName []byte) error {
			b := tx.Bucket(bucketName)
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			curs := b.Cursor()
			for k, _ := curs.Seek(Itob(min)); k != nil; k, _ = curs.Next() {
				if Btoi(k) > max {
					break
				}

				if err := curs.Delete(); err != nil {
					return err
				}
			}
			return nil
		})
	return err
}

// ================================================================= //
