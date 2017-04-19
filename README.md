# kvraft

kvraft is a key/value system that I want to implement.
It bases on Raft algorithm, uses Boltdb, gRPC and a web framework "gin"

## Install

- install
```sh
   go get -u github.com/hashicorp/raft
   go get -u github.com/boltdb/bolt
   go get -u github.com/gin-gonic/gin
   go get -u google.golang.org/grpc
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

- start cluster
```
   sh test.sh
```


