# kvraft

kvraft is a key/value service based on hashicorp/raft.
It uses Boltdb for storage, gRPC for cluster communication.

## Install

- install
```sh
   go get -u github.com/cxt90730/kvraft
   go get -a github.com/golang/protobuf/protoc-gen-go
```

*Notice*:
> if can not compile it, please do that (may sure protoc is installed):
> protoc --go_out=plugins=grpc:. op.proto

- build
```css
   go build
```

- start after go build is successful

*Notice*:
> The configure file must be json format
> The example is "./conf/kvraft*.conf"
```
   ./kvraft -c ./conf/kvraft.conf
```

## Example
```
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

	err = s.StopKVRaftService()
	if err != nil {
		t.Fatal(err.Error())
		return
	}
```

