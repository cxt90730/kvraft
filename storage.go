package kvraft

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/hashicorp/raft"
	"strconv"
	"sync"
)

var (
	STABLE_STORE_BUCKET        = []byte("stable_store")
	STABLE_STORE_UINT64_BUCKET = []byte("stable_uint64_store")

	LOG_STORE_BUCKET = []byte("log_store")
	KEY_FIRST_INDEX  = []byte("first_index")
	KEY_LAST_INDEX   = []byte("last_index")
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
	var valByte []byte
	return rs.rdb.SetValue(STABLE_STORE_UINT64_BUCKET, key, strconv.AppendUint(valByte, val, 10))
}

// GetUint64 implements the StableStore interface.
func (rs *RaftStorage) GetUint64(key []byte) (uint64, error) {
	valByte, err := rs.rdb.GetValue(STABLE_STORE_UINT64_BUCKET, key)
	if err != nil {
		return 0, err
	}
	//Empty value
	if valByte == nil {
		return 0, nil
	}
	val, err := strconv.ParseUint(string(valByte), 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// ================================================================= //

// ================ LogStore Interface ================ //

// FirstIndex returns the first index written. 0 for no entries.
func (rs *RaftStorage) FirstIndex() (uint64, error) {
	val, err := rs.rdb.GetValue(LOG_STORE_BUCKET, KEY_FIRST_INDEX)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return Btoi(val), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (rs *RaftStorage) LastIndex() (uint64, error) {
	val, err := rs.rdb.GetValue(LOG_STORE_BUCKET, KEY_LAST_INDEX)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	return Btoi(val), nil
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
	entry, err := json.Marshal(log)
	if err != nil {
		return err
	}
	err = rs.rdb.DoBatch(LOG_STORE_BUCKET,
		func(tx *bolt.Tx, bucketName []byte) error {
			b := tx.Bucket(bucketName)
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			logIndexByte := Itob(log.Index)

			err = b.Put(logIndexByte, entry)
			if err != nil {
				return err
			}
			firstIndex := b.Get(KEY_FIRST_INDEX)
			lastIndex := b.Get(KEY_LAST_INDEX)
			//if no index now, set index
			if firstIndex == nil || lastIndex == nil {
				err = b.Put(KEY_FIRST_INDEX, logIndexByte)
				if err != nil {
					return err
				}
				err = b.Put(KEY_LAST_INDEX, logIndexByte)
				if err != nil {
					return err
				}
			} else {
				lastIndexInt := Btoi(lastIndex)
				if log.Index > lastIndexInt {
					b.Put(KEY_LAST_INDEX, logIndexByte)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	return err
}

// StoreLogs stores multiple log entries.
func (rs *RaftStorage) StoreLogs(logs []*raft.Log) error {
	var err error
	for _, log := range logs {
		err = rs.StoreLog(log)
		if err != nil {
			break
		}
	}
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
			for i := min; i <= max; i++ {
				err := b.Delete(Itob(i))
				if err != nil {
					return err
				}
			}
			firstIndex := b.Get(KEY_FIRST_INDEX)
			firstIndexInt := Btoi(firstIndex)
			firstIndexInt = max + 1
			err := b.Put(KEY_FIRST_INDEX, Itob(firstIndexInt))
			if err != nil {
				return err
			}
			return nil
		})
	return err
}

// ================================================================= //
