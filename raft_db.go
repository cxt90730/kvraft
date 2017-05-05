package kvraft

import (
	"fmt"
	"github.com/boltdb/bolt"
	"os"
)

type RaftDB struct {
	Db *bolt.DB
}

const DB_MODE = 0666

func NewRaftDB(DbPath string, Durable bool) (*RaftDB, error) {
	//Remove old db if not durable
	if !Durable {
		if err := os.RemoveAll(DbPath); err != nil {
			return nil, err
		}
	}

	db, err := bolt.Open(DbPath, DB_MODE, nil)
	if err != nil {
		return nil, err
	}

	rdb := &RaftDB{db}
	err = rdb.initialRaftStore()
	if err != nil {
		rdb.Close()
		return nil, err
	}

	return rdb, nil
}

func (rdb *RaftDB) initialRaftStore() error {
	tx, err := rdb.Db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	//Initial bucket of raft store
	if _, err = tx.CreateBucketIfNotExists(LOG_STORE_BUCKET); err != nil {
		return err
	}
	if _, err = tx.CreateBucketIfNotExists(STABLE_STORE_BUCKET); err != nil {
		return err
	}
	return tx.Commit()
}

func (rdb *RaftDB) CreateBucket(bucketName []byte) error {
	return createBucket(rdb, bucketName)
}

func createBucket(rdb *RaftDB, bucketName []byte) error {
	err := rdb.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return fmt.Errorf("create bucket %s error : %s", string(bucketName), err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (rdb *RaftDB) SetValue(bucketName, key, value []byte) error {
	err := rdb.Db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket(bucketName)
		if b == nil {
			return bolt.ErrBucketNotFound
		}
		err = b.Put(key, value)
		return err
	})
	if err != nil {
		return fmt.Errorf("SetValue error : %s !", err.Error())
	}
	return nil
}

func (rdb *RaftDB) DeleteValue(bucketName, key []byte) error {
	err := rdb.Db.Update(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket(bucketName)
		if b == nil {
			return bolt.ErrBucketNotFound
		}
		err = b.Delete(key)
		return err
	})
	if err != nil {
		return fmt.Errorf("DeleteValue error : %s !", err.Error())
	}
	return nil
}

func (rdb *RaftDB) DoBatch(bucketName []byte, doBatch func(tx *bolt.Tx, bucketName []byte) error) error {
	err := rdb.Db.Batch(func(tx *bolt.Tx) error {
		err := doBatch(tx, bucketName)
		return err
	})
	if err != nil {
		return fmt.Errorf("DoBatch error : %s !", err.Error())
	}
	return nil
}

func (rdb *RaftDB) GetValue(bucketName, key []byte) ([]byte, error) {
	var value []byte
	err := rdb.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return bolt.ErrBucketNotFound
		}
		value = b.Get(key)
		return nil
	})
	return value, err
}

func (rdb *RaftDB) GetFirst(bucketName []byte) ([]byte, []byte, error) {
	tx, err := rdb.Db.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(bucketName).Cursor()
	k, v := curs.First()
	return k, v, nil
}

func (rdb *RaftDB) GetLast(bucketName []byte) ([]byte, []byte, error) {
	tx, err := rdb.Db.Begin(false)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	curs := tx.Bucket(bucketName).Cursor()
	k, v := curs.Last()
	return k, v, nil
}

func (rdb *RaftDB) Close() error {
	return rdb.Db.Close()
}
