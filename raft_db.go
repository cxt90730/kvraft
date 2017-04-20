package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"os"
)

type RaftDB struct {
	DbPath string
	Db     *bolt.DB
}

func (rdb *RaftDB) NewRaftDB() error {
	//Remove old db
	if err := os.RemoveAll(rdb.DbPath); err != nil {
		return err
	}
	err := rdb.InitDB()
	if err != nil {
		return err
	}
	err = rdb.createBucket(LOG_STORE_BUCKET)
	if err != nil {
		return err
	}
	err = rdb.createBucket(STABLE_STORE_BUCKET)
	if err != nil {
		return err
	}
	err = rdb.createBucket(STABLE_STORE_UINT64_BUCKET)
	if err != nil {
		return err
	}
	if len(serverConfig.BucketName) != 0 {
		err = rdb.createBucket([]byte(serverConfig.BucketName))
		if err != nil {
			return err
		}
	}

	return nil
}

func (rdb *RaftDB) InitDB() error {
	db, err := bolt.Open(rdb.DbPath, 0666, nil)
	if err != nil {
		return err
	}
	rdb.Db = db
	return nil
}

func (rdb *RaftDB) createBucket(bucketName []byte) error {
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
