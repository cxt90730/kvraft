package kvraft

type StorageDB interface {
	InitDB() error
	CreateBucket(bucketName []byte) error
	SetValue(bucketName, key, value []byte) error
	GetValue(bucketName, key []byte) ([]byte, error)
	DeleteValue(bucketName, key []byte) error
	Close() error
}
