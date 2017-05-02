package kvraft

//
//import (
//	"encoding/json"
//	"fmt"
//	"github.com/boltdb/bolt"
//	"github.com/fvbock/endless"
//	"github.com/gin-gonic/gin"
//	"io/ioutil"
//	"net/http"
//	"strings"
//	"time"
//    "bytes"
//    "io"
//    "os"
//)
//
////TODO:  Encrypt
//
//const  (
//
//    SHARE_LEADER_INFO_TIMEOUT = 1 * time.Second
//
//)
//
//type KVRaftServer struct {
//    Router      *gin.Engine
//    Service     *KVRaftService
//    Config      *ServerConfig
//}
//
//func StartKVRaftServer(router *gin.Engine, config *ServerConfig, logOutput io.Writer) {
//
//    kv
//    kvService := NewKVRaftService()
//
//    localServer = &KVRaftServer{
//        Router:      router,
//        Config:,
//    }
//
//
//
//    go localServer.ShareServerConfig()
//
//    go localServer.JoinServerMember()
//
//    err = localServer.Start()
//    if err != nil {
//        panic(err)
//    }
//}
//
//
//
//
////Handler
//
//func SetHandler(ctx *gin.Context) {
//	bucketName := ctx.Param("bucket")
//	key := ctx.Param("key")
//    if bucketName == "" || key == "" {
//        ctx.JSON(http.StatusBadRequest, "lack of bucket or key")
//        return
//    }
//
//	value, err := ioutil.ReadAll(ctx.Request.Body)
//	if err != nil || value == nil || len(value) == 0 {
//		ctx.JSON(http.StatusBadRequest, "lack of binary data")
//		return
//	}
//	rpcCmd := RpcCmd{
//		Op:     CmdSet,
//		Key:    []byte(key),
//		Bucket: []byte(bucketName),
//		Value:  value,
//	}
//
//	err = rpcCmd.DoSet()
//	if err != nil {
//		if err == bolt.ErrBucketNotFound {
//			ctx.JSON(http.StatusNotFound, "no target bucket:"+bucketName)
//		} else {
//			ctx.JSON(http.StatusInternalServerError, "error in set value:"+err.Error())
//		}
//		return
//	}
//	ctx.JSON(http.StatusOK, "set value success")
//
//}
//
//func GetHandler(ctx *gin.Context) {
//	bucketName := ctx.Param("bucket")
//	key := ctx.Param("key")
//	rpcCmd := RpcCmd{
//		Op:     CmdGet,
//		Key:    []byte(key),
//		Bucket: []byte(bucketName),
//	}
//
//	if bucketName == "" || key == "" {
//		ctx.JSON(http.StatusBadRequest, "lack of bucket or key")
//		return
//	}
//
//	value, err := rpcCmd.DoGet()
//	if err != nil {
//		if err == bolt.ErrBucketNotFound {
//			ctx.JSON(http.StatusNotFound, "no target bucket:"+bucketName)
//		} else {
//			ctx.JSON(http.StatusInternalServerError, "error in get value:"+err.Error())
//		}
//		return
//	}
//
//	if value == nil {
//		ctx.JSON(http.StatusNotFound, "no target value, key is:"+key)
//		return
//	}
//
//	ctx.JSON(http.StatusOK, string(value))
//}
//
//func GetPeerWithHashHandler(ctx *gin.Context) {
//	data := ctx.Param("data")
//	ipPort, err := getPeerWithHash([]byte(data))
//	if err != nil {
//		ctx.JSON(http.StatusInternalServerError, "err:"+err.Error())
//		return
//	}
//	ip := strings.Split(ipPort, ":")[0]
//	ctx.JSON(http.StatusOK, ip)
//}
//
//func getPeerWithHash(data []byte) (string, error) {
//	peers, err := localServer.PeerStorage.Peers()
//	localServer.Raft.Leader()
//	if err != nil {
//		return "", err
//	}
//	count := len(peers)
//	if count == 0 {
//		return "", fmt.Errorf("No peer in raft")
//	}
//	index := hash(data) % uint64(count)
//	return peers[index], nil
//}
//
//
//func join(peer string) error {
//    if peer == "" {
//        return fmt.Errorf("peer is invalid")
//    }
//    //Join must be invoked on leader
//    leader := localServer.Raft.Leader()
//    if leader == "" {
//        return fmt.Errorf("No Leader In Cluster")
//    }
//    if leader != localServer.Config.RaftAddrString() {
//        //notify leader
//        httpURL := fmt.Sprintf("http://%s/peer/set", ShCache.LeaderHTTPAddr + ":" + ShCache.LeaderHTTPPort)
//        resp, err := http.Post(httpURL, "application/octet-stream", bytes.NewReader([]byte(peer)))
//        if err != nil {
//            return err
//        }
//        defer resp.Body.Close()
//
//        if resp.StatusCode != http.StatusOK {
//            return fmt.Errorf("request status code: %d", resp.StatusCode)
//        }
//        return nil
//    }
//
//    //Join on leader
//    future := localServer.Raft.AddPeer(peer)
//    if err := future.Error(); err != nil {
//        return err
//    }
//    return nil
//}
//
//func JoinPeer(ctx *gin.Context){
//    peer, err := ioutil.ReadAll(ctx.Request.Body)
//    if err != nil || peer == nil || len(peer) == 0 {
//        ctx.JSON(http.StatusBadRequest, "lack of binary data")
//        return
//    }
//    err = join(string(peer))
//    if err != nil {
//        ServerLogger.Println("err:", err.Error())
//        ctx.JSON(http.StatusInternalServerError, "error in join peer: "+err.Error())
//        return
//    }
//    ctx.JSON(http.StatusOK, "Join peer " + string(peer) + " success!")
//}
//
//
//func GetState(ctx *gin.Context) {
//    leader := localServer.Raft.Leader()
//    statMap := localServer.Raft.Stats()
//    statString := fmt.Sprintf("leader : %s Http: %s:%s \n", leader, ShCache.LeaderHTTPAddr, ShCache.LeaderHTTPPort)
//    peers, err := localServer.PeerStorage.Peers()
//    if err != nil {
//        ctx.JSON(http.StatusInternalServerError, "error in get peer: " + err.Error())
//        return
//    }
//    for i, peer := range peers {
//        statString += fmt.Sprintf("peer%d : %s \n", i, peer)
//    }
//    activeMembers := localServer.Member.Members()
//    statString += fmt.Sprintf("ActiveMembers: \n")
//    for _, member := range activeMembers {
//        statString += fmt.Sprintf("%s ; ", member.Address())
//    }
//    statString += fmt.Sprintf("\n")
//    for k, v := range statMap {
//        statString = statString + fmt.Sprintf("%s : %s \n", k, v)
//        fmt.Println(statString)
//    }
//    ctx.String(http.StatusOK, statString)
//}
//
//
//type ServerConfig struct {
//    ServerAddr       string
//    ServerPort       string
//    LogDir           string
//    KVRConfig        KVRaftConfig
//}
//
//func (sc *ServerConfig) ServerAddrString() string {
//    return fmt.Sprintf("%s:%s", sc.ServerAddr, sc.ServerPort)
//}
////Conf content must be formatted by json
//
//var serverConfig *ServerConfig
//
//func NewServerConfig(confPath string) (*ServerConfig, error) {
//    serverConfig = &ServerConfig{}
//    cFile, err := os.Open(confPath)
//    if err != nil {
//        return nil, err
//    }
//    defer cFile.Close()
//    err = json.NewDecoder(cFile).Decode(serverConfig)
//    if err != nil {
//        return nil, err
//    }
//    return serverConfig, nil
//}
//
//func (server *KVRaftServer) Start() error {
//
//	server.Router.PUT("/api/:bucket/:key", SetHandler)
//	server.Router.GET("/api/:bucket/:key", GetHandler)
//	server.Router.GET("/peer/get/:data", GetPeerWithHashHandler)
//    server.Router.POST("/peer/set", JoinPeer)
//    server.Router.GET("/peer/get", GetState)
//	endless.ListenAndServe(":"+server.Config.ServerPort, server.Router)
//
//	return nil
//}
