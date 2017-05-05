package kvraft

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	WAIT_LEADER_TIMEOUT       = 500 * time.Millisecond
	DEFAULT_PEER_STORAGE_PATH = "peer.json"
	DEFAULT_DB_DIR            = "./db/"
	DB_NAME                   = "kvraft.db"
	RAFT_LOG_NAME             = "kvraft.log"
	DEFAULT_RAFT_LOGDIR       = "./kvraftLog/"
	SHARE_LEADER_INFO_TIMEOUT = 1 * time.Second
)

type KVRaftService struct {
	db     StorageDB
	log    *log.Logger
	fsm    *RaftFSM
	raft   *raft.Raft
	trans  raft.Transport
	config *KVRaftConfig
	peer   raft.PeerStore
	member *memberlist.Memberlist
}

type ShareCache struct {
	LeaderRpcAddr    string
	LeaderRpcPort    string
	LeaderMemberAddr string
	LeaderMemberPort int
}

var ShCache *ShareCache

func InitShareCache() {
	ShCache = &ShareCache{}
}

func NewKVRaftService(config *KVRaftConfig) (*KVRaftService, error) {

	var raftLogDir, dbDir, peerStoragePath string
	var err error

	peerStoragePath, err = VerifyDir(config.PeerStorage, DEFAULT_PEER_STORAGE_PATH)
	if err != nil {
		return nil, err
	}
	dbDir, err = VerifyDir(config.DbDir, DEFAULT_DB_DIR)
	if err != nil {
		return nil, err
	}
	raftLogDir, err = VerifyDir(config.RaftLogDir, DEFAULT_RAFT_LOGDIR)
	if err != nil {
		return nil, err
	}

	raftAddr := fmt.Sprintf("%s:%s", config.RaftAddr, config.RaftPort)

	raftLogFile, err := os.OpenFile(raftLogDir+RAFT_LOG_NAME, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	trans, err := raft.NewTCPTransport(raftAddr, nil, 3, 2*time.Second, raftLogFile)
	if err != nil {
		return nil, err
	}

	kvraftLogger := InitServerLogger(raftLogFile, "[kvraft]")
	InitShareCache()

	//============== Raft Config ==============//
	rc := raft.DefaultConfig()

	dbPath := dbDir + DB_NAME
	//RaftDb implements boltDb
	raftDB, err := NewRaftDB(dbPath, config.Durable)
	if err != nil {
		return nil, err
	}

	rStorage := &RaftStorage{rdb: raftDB, mu: &sync.Mutex{}}

	//FSM implementation
	raftFSM := NewStorageFSM(raftDB, kvraftLogger)
	//TODO: snapshot

	//Snapshot implementation
	snap, err := raft.NewFileSnapshotStore(config.SnapshotStorage, 3, nil)

	//TCP implementation

	peerStorage := raft.NewJSONPeers(peerStoragePath, trans)
	ps, err := peerStorage.Peers()

	if len(ps) <= 1 && config.EnableSingleNode {
		kvraftLogger.Println("Into SingleNode.")
		rc.EnableSingleNode = true
		rc.DisableBootstrapAfterElect = false
	}
	peerStorage.SetPeers(config.Peers)

	r, err := raft.NewRaft(rc, raftFSM, rStorage, rStorage, snap, peerStorage, trans)
	if err != nil {
		return nil, err
	}

	//memberlist
	//LAN Configure
	mListConf := memberlist.DefaultLANConfig()
	if config.MemberPort == 0 {
		return nil, fmt.Errorf("Invalid memberlist port!")
	}
	mListConf.BindAddr = config.MemberAddr
	mListConf.BindPort = config.MemberPort
	mListConf.Logger = kvraftLogger
	if config.MemberName == "" {
		mListConf.Name = config.RpcAddrString()
	} else {
		mListConf.Name = config.MemberName
	}

	memList, err := memberlist.Create(mListConf)
	if err != nil {
		return nil, err
	}

	kvService := &KVRaftService{
		log:    kvraftLogger,
		db:     raftDB,
		config: config,
		trans:  trans,
		fsm:    raftFSM,
		peer:   peerStorage,
		raft:   r,
		member: memList,
	}

	go func() {
		_, err := NewRPCServer(config.RpcAddrString(), kvService)
		if err != nil {
			panic(err)
		}
	}()
	go shareConfig(kvService, SHARE_LEADER_INFO_TIMEOUT)

	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(1)
	go joinServerMember(kvService, waitGroup)

	kvraftLogger.Println("waiting leader!")
	for r.Leader() == "" || ShCache.LeaderRpcAddr == "" || ShCache.LeaderRpcPort == "" {
		time.Sleep(WAIT_LEADER_TIMEOUT)
	}
	kvraftLogger.Println("Leader has elected! Raft:", r.Leader(), " Rpc:", ShCache.LeaderRpcAddr, ":", ShCache.LeaderRpcPort)

	waitGroup.Wait()
	kvraftLogger.Println("kvraft service start!")

	return kvService, nil
}

func joinServerMember(s *KVRaftService, waitGroup *sync.WaitGroup) {
	s.log.Println("Join in member list now!")
	for {
		if ShCache.LeaderMemberPort == 0 || ShCache.LeaderMemberAddr == "" {
			time.Sleep(SHARE_LEADER_INFO_TIMEOUT)
			continue
		}
		s.log.Println("Ready to join member list: ", ShCache.LeaderMemberAddr, ":", ShCache.LeaderMemberPort)
		_, err := s.member.Join([]string{ShCache.LeaderMemberAddr + ":" + strconv.Itoa(ShCache.LeaderMemberPort)})
		if err != nil {
			s.log.Println("Can not join in member list : ", err.Error())
			time.Sleep(SHARE_LEADER_INFO_TIMEOUT)
			continue
		}
		s.log.Println("Join in member list success !")
		break
	}
	waitGroup.Done()
}

func shareConfig(s *KVRaftService, timeout time.Duration) {
	for {
		//Get Leader, if leader is self , notify others
		time.Sleep(timeout)
		if s.config.RaftAddrString() == s.raft.Leader() {
			// update leader address info
			ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort = s.config.RpcAddr, s.config.RpcPort
			ShCache.LeaderMemberAddr, ShCache.LeaderMemberPort = s.config.MemberAddr, s.config.MemberPort
			value, _ := json.Marshal(ShCache)
			opReq := OpRequest{
				Op:    CmdShare,
				Key:   "",
				Value: value,
			}
			cmd, _ := json.Marshal(opReq)
			err := s.raft.Apply(cmd, 3)
			if err != nil && err.Error() != nil {
				s.log.Println("Share config apply error: ", err.Error())
				continue
			}
			s.log.Println("Share config apply success! ")
		}
		s.config.LeaderRpcAddr, s.config.LeaderRpcPort = ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort
	}
}

func (s *KVRaftService) GetKVRaftState() (string, error) {
	leader := s.raft.Leader()
	statMap := s.raft.Stats()
	statString := fmt.Sprintf("leader : %s Rpc: %s:%s \n", leader, ShCache.LeaderRpcAddr, ShCache.LeaderRpcPort)
	peers, err := s.peer.Peers()
	if err != nil {
		return "", fmt.Errorf("error in get peer: %s ", err.Error())
	}
	for i, peer := range peers {
		statString += fmt.Sprintf("peer%d : %s \n", i, peer)
	}
	activeMembers := s.member.Members()
	statString += fmt.Sprintf("ActiveMembers: \n")
	for _, member := range activeMembers {
		statString += fmt.Sprintf("%s ; ", member.String())
	}
	statString += fmt.Sprintf("\n")
	for k, v := range statMap {
		statString = statString + fmt.Sprintf("%s : %s \n", k, v)
	}
	return statString, nil
}

func (s *KVRaftService) GetMemberList() []string {
	activeMembers := s.member.Members()
	members := make([]string, len(activeMembers))
	for i, member := range activeMembers {
		members[i] = member.String()
	}
	return members
}

func (s *KVRaftService) StopKVRaftService() error {
	future := s.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}
	return s.db.Close()
}
