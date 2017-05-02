package kvraft

import (
	"fmt"
	"testing"
	"time"
)

func Test_SingleKVRaftService(t *testing.T) {
	conf, err := NewKVRaftConfig("./conf/kvraft_single.conf")
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	s, err := NewKVRaftService(conf)
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	bucketName := []byte("haha")
	err = s.CreateBucket(bucketName)
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	err = s.SetValue(bucketName, []byte("hehe"), []byte("fuck"))
	if err != nil {
		t.Fatal(err.Error())
		return
	}

	v, err := s.GetValue(bucketName, []byte("hehe"))
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	if string(v) != "fuck" {
		t.Fatal(fmt.Errorf("not correct value!"))
		return
	}

	err = s.StopKVRaftService()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
}

func Test_3ClusterService(t *testing.T) {
	n := 3
	var confMap []*KVRaftConfig
	var err error
	confMap = make([]*KVRaftConfig, n)
	for i := 0; i < n; i++ {
		confMap[i], err = NewKVRaftConfig(fmt.Sprintf("./conf/kvraft%d.conf", i))
		if err != nil {
			t.Fatal(err.Error())
			return
		}
	}
	serviceMap := make(map[int]*KVRaftService)
	fmt.Println("asasd", n, len(serviceMap))
	for i := 0; i < n; i++ {

		go func() {
			fmt.Println(i)
			serviceMap[i], err = NewKVRaftService(confMap[i])
		}()
		time.Sleep(5 * time.Second)
		if err != nil {
			t.Fatal(err.Error())
			return
		}
	}
	bucketName := []byte("lr")
	key := []byte("lr")
	value := []byte("lr")
	err = serviceMap[0].CreateBucket(bucketName)
	if err != nil {
		fmt.Println("Raft Leader is:", serviceMap[0].raft.Leader())
		t.Fatal(err.Error())
		return
	}

	err = serviceMap[1].SetValue(bucketName, key, value)
	if err != nil {
		fmt.Println("NO BUCKET!")
		time.Sleep(1 * time.Second)
	}
	err = serviceMap[1].SetValue(bucketName, key, value)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	v, err := serviceMap[2].GetValue(bucketName, key)
	if err != nil {
		t.Fatal(err.Error())
		return
	}
	if string(v) != string(value) {
		t.Fatal(fmt.Errorf("not correct value!"))
		return
	}
}
