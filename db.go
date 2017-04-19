package main

type StorageDB interface {
	InitDB() error
	SetValue(container, key, value []byte) error
	GetValue(container, key []byte) ([]byte, error)
	DeleteValue(container, key []byte) error
}
