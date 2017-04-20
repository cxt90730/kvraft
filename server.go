package main

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fvbock/endless"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

//TODO:  Encrypt

//Global Server
var localServer *KVRaftServer

//Global Logger
var ServerLogger *log.Logger

// Local Cache
var ShCache *ShareCache

type ShareCache struct {
	LeaderRpcAddr  string
	LeaderRpcPort  string
	LeaderHTTPAddr string
	LeaderHTTPPort string
}

func InitShareCache() {
	ShCache = &ShareCache{}
}

type KVRaftServer struct {
	Router      *gin.Engine
	Db          StorageDB
	Log         *log.Logger
	FSM         *RaftFSM
	Raft        *raft.Raft
	Trans       raft.Transport
	Config      *ServerConfig
	PeerStorage raft.PeerStore
}

func (s *KVRaftServer) ShareServerConfig() {
	for {
		time.Sleep(time.Second * 3)

		//Get Leader, if leader is self , notify others
		if s.Config.RaftAddrString() == s.Raft.Leader() {
			// update leader address info
			opReq := OpRequest{
				Op:    CmdShare,
				Key:   "",
				Value: []byte(s.Config.RpcAddrString()),
			}
			ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort = s.Config.RpcAddr, s.Config.RpcPort
			opReq.Value, _ = json.Marshal(ShCache)
			cmd, _ := json.Marshal(opReq)
			err := s.Raft.Apply(cmd, 3)
			if err != nil && err.Error() != nil {
				ServerLogger.Println("Share config apply error: ", err)
				continue
			}
			time.Sleep(time.Second * 5)
		}

		s.Config.LeaderAddr, s.Config.LeaderPort = ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort
	}
}

func SetHandler(ctx *gin.Context) {
	bucketName := ctx.Param("bucket")
	key := ctx.Param("key")
	value, err := ioutil.ReadAll(ctx.Request.Body)
	if bucketName == "" || key == "" {
		ctx.JSON(http.StatusBadRequest, "lack of bucket or key")
		return
	}

	if err != nil || value == nil || len(value) == 0 {
		ctx.JSON(http.StatusBadRequest, "lack of data binary")
		return
	}
	rpcCmd := RpcCmd{
		Op:     CmdSet,
		Key:    []byte(key),
		Bucket: []byte(bucketName),
		Value:  value,
	}

	err = rpcCmd.DoSet()
	if err != nil {
		if err == bolt.ErrBucketNotFound {
			ctx.JSON(http.StatusNotFound, "no target bucket:"+bucketName)
		} else {
			ctx.JSON(http.StatusInternalServerError, "error in set value:"+err.Error())
		}
		return
	}
	ctx.JSON(http.StatusOK, "set value success")

}

func GetHandler(ctx *gin.Context) {
	bucketName := ctx.Param("bucket")
	key := ctx.Param("key")
	rpcCmd := RpcCmd{
		Op:     CmdGet,
		Key:    []byte(key),
		Bucket: []byte(bucketName),
	}

	if bucketName == "" || key == "" {
		ctx.JSON(http.StatusBadRequest, "lack of bucket or key")
		return
	}

	value, err := rpcCmd.DoGet()
	if err != nil {
		if err == bolt.ErrBucketNotFound {
			ctx.JSON(http.StatusNotFound, "no target bucket:"+bucketName)
		} else {
			ctx.JSON(http.StatusInternalServerError, "error in get value:"+err.Error())
		}
		return
	}

	if value == nil {
		ctx.JSON(http.StatusNotFound, "no target value, key is:"+key)
		return
	}

	ctx.JSON(http.StatusOK, string(value))
}

func GetPeerWithHashHandler(ctx *gin.Context) {
	data := ctx.Param("data")
	ipPort, err := getPeerWithHash([]byte(data))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, "err:"+err.Error())
		return
	}
	ip := strings.Split(ipPort, ":")[0]
	ctx.JSON(http.StatusOK, ip)
}

func getPeerWithHash(data []byte) (string, error) {
	peers, err := localServer.PeerStorage.Peers()
	localServer.Raft.Leader()
	if err != nil {
		return "", err
	}
	count := len(peers)
	if count == 0 {
		return "", fmt.Errorf("No peer in raft")
	}
	index := hash(data) % count
	return peers[index], nil
}

func (server *KVRaftServer) Start() error {
	ServerLogger = server.Log
	localServer = server

	server.Router.PUT("/api/:bucket/:key", SetHandler)
	server.Router.GET("/api/:bucket/:key", GetHandler)
	server.Router.GET("/peer/get/:data", GetPeerWithHashHandler)
	endless.ListenAndServe(":"+server.Config.ServerPort, server.Router)

	return nil
}
