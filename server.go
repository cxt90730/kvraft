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
    "bytes"
    "sync"
    "os"
    "io"
    "github.com/hashicorp/memberlist"
    "strconv"
)

//TODO:  Encrypt

const  (
    WAIT_LEADER_TIMEOUT = 500 * time.Millisecond
    SHARE_LEADER_INFO_TIMEOUT = 3 * time.Second
)
// Local Cache
var ShCache *ShareCache

type KVRaftServer struct {
    Router      *gin.Engine
    Db          StorageDB
    Log         *log.Logger
    FSM         *RaftFSM
    Raft        *raft.Raft
    Trans       raft.Transport
    Config      *ServerConfig
    PeerStorage raft.PeerStore
    Member      *memberlist.Memberlist
}

func StartKVRaftServer(router *gin.Engine, config *ServerConfig, logOutput io.Writer) {
    //============== ============== ==============//

    //Rpc config
    go func() {
        fmt.Println("Ready to Start RPC Server! Addr: ", config.RpcAddrString())
        _, err := NewRPCServer(config.RpcAddrString())
        if err != nil {
            panic(err)
        }
        fmt.Println("RPC Server Start!")
        ServerLogger.Println("RPC Server Start!")
    }()

    InitShareCache()

    //============== Raft Config ==============//
    rc := raft.DefaultConfig()
    var dbPath, peerStoragePath string
    //TODO:
    if config.DbPath == "" {
        dbPath = DEFAULT_DB_PATH
    } else {
        dbPath = config.DbPath
    }
    //RaftDb implements boltDb
    raftDB := &RaftDB{DbPath: dbPath}
    err := raftDB.NewRaftDB()
    if err != nil {
        panic(err)
    }
    defer raftDB.Db.Close()

    rStorage := &RaftStorage{rdb: raftDB, mu: &sync.Mutex{}}

    //FSM implementation
    raftFSM := NewStorageFSM(raftDB)
    //TODO: snapshot

    //Snapshot implementation
    snap, err := raft.NewFileSnapshotStore(config.SnapshotStorage, 3, nil)

    //TCP implementation
    raftAddr := fmt.Sprintf("%s:%s", config.RaftAddr, config.RaftPort)
    trans, err := raft.NewTCPTransport(raftAddr, nil, 3, 2*time.Second, logOutput)
    if err != nil {
        panic(err)
    }

    if config.PeerStorage == "" {
        peerStoragePath = DEFAULT_PEER_STORAGE_PATH
    } else {
        peerStoragePath = config.PeerStorage
    }

    //Set Peer
    err = os.MkdirAll(peerStoragePath, 0755)
    if err != nil {
        panic(err)
    }

    peerStorage := raft.NewJSONPeers(peerStoragePath, trans)
    ps, err := peerStorage.Peers()
    if len(ps) <= 1 && config.EnableSingleNode {
        rc.EnableSingleNode = true
        rc.DisableBootstrapAfterElect = false
    }
    peerStorage.SetPeers(config.Peers)

    ps, _ = peerStorage.Peers()
    fmt.Println(ps)

    r, err := raft.NewRaft(rc, raftFSM, rStorage, rStorage, snap, peerStorage, trans)
    if err != nil {
        panic("raft error:" + err.Error())
    }

    //memberlist
    //LAN Configure
    mListConf := memberlist.DefaultLANConfig()
    if config.MemberBindPort == 0 {
        panic(fmt.Errorf("Invalid memberlist port!"))
    }
    mListConf.BindAddr = config.ServerAddr
    mListConf.BindPort = config.MemberBindPort
    mListConf.Logger = ServerLogger
    memList, err := memberlist.Create(mListConf)
    if err != nil {
        panic(err)
    }

    //============== ============== ==============//

    localServer = &KVRaftServer{
        Router:      router,
        Log:         ServerLogger,
        Db:          raftDB,
        Config:      config,
        Trans:       trans,
        FSM:         raftFSM,
        PeerStorage: peerStorage,
        Raft:        r,
        Member:      memList,
    }

    for r.Leader() == "" {
        ServerLogger.Println("waiting leader!")
        time.Sleep(WAIT_LEADER_TIMEOUT)
    }
    ServerLogger.Println("leader has elected:", r.Leader())

    go localServer.ShareServerConfig()

    go localServer.JoinServerMember()

    err = localServer.Start()
    if err != nil {
        panic(err)
    }
}


type ShareCache struct {
	LeaderRpcAddr  string
	LeaderRpcPort  string
	LeaderHTTPAddr string
	LeaderHTTPPort string
    LeaderMemberPort int
}

func InitShareCache() {
	ShCache = &ShareCache{}
}

func (s *KVRaftServer) JoinServerMember() {
    for {
        if ShCache.LeaderMemberPort == 0 || ShCache.LeaderHTTPAddr == ""{
            ServerLogger.Println("Can not join in member list now!")
            time.Sleep(WAIT_LEADER_TIMEOUT)
            continue
        }
        ServerLogger.Println("Ready to join member list: ", ShCache.LeaderHTTPAddr, ":", ShCache.LeaderMemberPort)
        _ , err := s.Member.Join([]string{ShCache.LeaderHTTPAddr + ":" + strconv.Itoa(ShCache.LeaderMemberPort)})
        if err != nil {
            ServerLogger.Println("Can not join in member list : ", err.Error())
            time.Sleep(WAIT_LEADER_TIMEOUT)
            continue
        }
        ServerLogger.Println("Join in member list success !")
        break
    }
}

func (s *KVRaftServer) ShareServerConfig() {
	for {
		//Get Leader, if leader is self , notify others
        time.Sleep(SHARE_LEADER_INFO_TIMEOUT)
		if s.Config.RaftAddrString() == s.Raft.Leader() {
			// update leader address info
            ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort = s.Config.RpcAddr, s.Config.RpcPort
            ShCache.LeaderHTTPAddr, ShCache.LeaderHTTPPort = s.Config.ServerAddr, s.Config.ServerPort
            ShCache.LeaderMemberPort = s.Config.MemberBindPort

            value, _ := json.Marshal(ShCache)
			opReq := OpRequest{
				Op:    CmdShare,
				Key:   "",
				Value: value,
			}
			cmd, _ := json.Marshal(opReq)
			err := s.Raft.Apply(cmd, 3)
			if err != nil && err.Error() != nil {
				ServerLogger.Println("Share config apply error: ", err.Error())
				continue
			}
            ServerLogger.Println("Share config apply success! ")
		}

		s.Config.LeaderRpcAddr, s.Config.LeaderRpcPort = ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort
	}
}

//Handler

func SetHandler(ctx *gin.Context) {
	bucketName := ctx.Param("bucket")
	key := ctx.Param("key")
    if bucketName == "" || key == "" {
        ctx.JSON(http.StatusBadRequest, "lack of bucket or key")
        return
    }

	value, err := ioutil.ReadAll(ctx.Request.Body)
	if err != nil || value == nil || len(value) == 0 {
		ctx.JSON(http.StatusBadRequest, "lack of binary data")
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
	index := hash(data) % uint64(count)
	return peers[index], nil
}


func join(peer string) error {
    if peer == "" {
        return fmt.Errorf("peer is invalid")
    }
    //Join must be invoked on leader
    leader := localServer.Raft.Leader()
    if leader == "" {
        return fmt.Errorf("No Leader In Cluster")
    }
    if leader != localServer.Config.RaftAddrString() {
        //notify leader
        httpURL := fmt.Sprintf("http://%s/peer/set", ShCache.LeaderHTTPAddr + ":" + ShCache.LeaderHTTPPort)
        resp, err := http.Post(httpURL, "application/octet-stream", bytes.NewReader([]byte(peer)))
        if err != nil {
            return err
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("request status code: %d", resp.StatusCode)
        }
        return nil
    }

    //Join on leader
    future := localServer.Raft.AddPeer(peer)
    if err := future.Error(); err != nil {
        return err
    }
    return nil
}

func JoinPeer(ctx *gin.Context){
    peer, err := ioutil.ReadAll(ctx.Request.Body)
    if err != nil || peer == nil || len(peer) == 0 {
        ctx.JSON(http.StatusBadRequest, "lack of binary data")
        return
    }
    err = join(string(peer))
    if err != nil {
        ServerLogger.Println("err:", err.Error())
        ctx.JSON(http.StatusInternalServerError, "error in join peer: "+err.Error())
        return
    }
    ctx.JSON(http.StatusOK, "Join peer " + string(peer) + " success!")
}


func GetState(ctx *gin.Context) {
    leader := localServer.Raft.Leader()
    statMap := localServer.Raft.Stats()
    statString := fmt.Sprintf("leader : %s Http: %s:%s \n", leader, ShCache.LeaderHTTPAddr, ShCache.LeaderHTTPPort)
    peers, err := localServer.PeerStorage.Peers()
    if err != nil {
        ctx.JSON(http.StatusInternalServerError, "error in get peer: " + err.Error())
        return
    }
    for i, peer := range peers {
        statString += fmt.Sprintf("peer%d : %s \n", i, peer)
    }
    activeMembers := localServer.Member.Members()
    statString += fmt.Sprintf("ActiveMembers: \n")
    for _, member := range activeMembers {
        statString += fmt.Sprintf("%s ; ", member.Address())
    }
    statString += fmt.Sprintf("\n")
    for k, v := range statMap {
        statString = statString + fmt.Sprintf("%s : %s \n", k, v)
        fmt.Println(statString)
    }
    ctx.String(http.StatusOK, statString)
}

func (server *KVRaftServer) Start() error {

	server.Router.PUT("/api/:bucket/:key", SetHandler)
	server.Router.GET("/api/:bucket/:key", GetHandler)
	server.Router.GET("/peer/get/:data", GetPeerWithHashHandler)
    server.Router.POST("/peer/set", JoinPeer)
    server.Router.GET("/peer/get", GetState)
	endless.ListenAndServe(":"+server.Config.ServerPort, server.Router)

	return nil
}
