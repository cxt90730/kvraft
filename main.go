package main

import (
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/raft"
	"os"
	"sync"
	"time"
    "strings"
)

const (
	DEFAULT_LOG_DIR   = "./log/"
	ACCESS_LOG_NAME   = "access.log"
	ERROR_LOG_NAME    = "error.log"
	SERVER_LOG_NAME   = "kvraft.log"
    DEFAULT_PEER_STORAGE_PATH = "peer.json"
	DEFAULT_DB_PATH   = "kv_raft.db"
	DEFAULT_CONF_PATH = "./conf/kvraft.conf"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	var cfgPath, joinAddr string
	flag.StringVar(&cfgPath, "c", DEFAULT_CONF_PATH, "conf file path")
	flag.StringVar(&joinAddr, "join", "", "join addr host:port")
	flag.Parse()

	//============== Server Config ==============//
	config, err := NewServerConfig(cfgPath)
	if err != nil {
		panic(err)
		return
	}
	//============== ============== ==============//

	//============== Create all Logs ==============//
	var logDir, dbPath, peerStoragePath string
	if config.LogDir == "" {
		logDir = DEFAULT_LOG_DIR
	} else {
		logDir = config.LogDir
	}
	if config.DbPath == "" {
		dbPath = DEFAULT_DB_PATH
	} else {
		dbPath = config.DbPath
	}
    if config.PeerStorage == "" {
        peerStoragePath = DEFAULT_PEER_STORAGE_PATH
    } else {
        peerStoragePath = config.PeerStorage
    }

	err = os.MkdirAll(logDir, 0755)
	if err != nil {
		panic(err)
	}

    if !strings.HasSuffix(logDir, "/") {
        logDir = logDir + "/"
    }
	serverLogFile, err := os.OpenFile(logDir+SERVER_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + logDir + SERVER_LOG_NAME + ":" + err.Error())
        return
	}
	defer serverLogFile.Close()

	// 建立 AccessLogger
	accessLogFile, err := os.OpenFile(logDir+ACCESS_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
        panic("Failed to open log file " + logDir + ACCESS_LOG_NAME + ":" + err.Error())
        return
    }
	defer accessLogFile.Close()

	// 建立 ErrorLogger
	errorLogFile, err := os.OpenFile(logDir+ERROR_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
        panic("Failed to open log file " + logDir + ERROR_LOG_NAME + ":" + err.Error())
        return
    }
	defer errorLogFile.Close()

	ServerLogger := InitServerLog(serverLogFile, "[kvraft]")

	//Use AccessLogger and ErrorLogger
	router.Use(LoggerWithWriter(accessLogFile, "[kvraft]"), gin.RecoveryWithWriter(errorLogFile))

	//============== ============== ==============//

	//Rpc config
	var rpcService *RaftRpcService

	go func() {
		fmt.Println("Ready to Start RPC Server! Addr: ", config.RpcAddrString())
		rpcService, err = NewRPCServer(config.RpcAddrString())
		if err != nil {
			panic(err)
		}
		fmt.Println("RPC Server Start!")
		ServerLogger.Println("RPC Server Start!")
	}()

	//============== Raft Config ==============//
	rc := raft.DefaultConfig()

	//TODO:join

	//RaftDb implements boltDb
	raftDB := &RaftDB{DbPath: dbPath}
	err = raftDB.NewRaftDB()
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
	trans, err := raft.NewTCPTransport(raftAddr, nil, 3, 2*time.Second, serverLogFile)
	if err != nil {
		panic(err)
		return
	}

	//Set Peer
    err = os.MkdirAll(peerStoragePath, 0755)
    if err != nil {
        panic(err)
    }

	peerStorage := raft.NewJSONPeers(peerStoragePath, trans)
	ps, err := peerStorage.Peers()
	if len(ps) <= 1 && config.EnableSingleNode{
		rc.EnableSingleNode = true
		rc.DisableBootstrapAfterElect = false
	}
	peerStorage.SetPeers(config.Peers)

    ps, _ = peerStorage.Peers()
    fmt.Println(ps)
	ServerLogger.Println("waiting leader!")
	r, err := raft.NewRaft(rc, raftFSM, rStorage, rStorage, snap, peerStorage, trans)
	if err != nil {
		panic("raft error:" + err.Error())
	}
	ServerLogger.Println("leader has elected:", r.Leader())

	localServer = &KVRaftServer{
		Router:      router,
		Log:         ServerLogger,
		Db:          raftDB,
		Config:      config,
		Trans:       trans,
		FSM:         raftFSM,
		PeerStorage: peerStorage,
		Raft:        r,
	}

	peers, _ := localServer.PeerStorage.Peers()
	ServerLogger.Println(localServer.Trans.LocalAddr(), " status is ", localServer.Raft.State().String())
	ServerLogger.Println(localServer.Trans.LocalAddr(), " 's peer is ", fmt.Sprintf("%+v", peers))

	InitShareCache()
	go localServer.ShareServerConfig()

	fmt.Println("Load conf: ", cfgPath)
	err = localServer.Start()
	if err != nil {
		panic(err)
	}
}
