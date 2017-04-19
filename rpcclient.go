package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"sync"
)

//TODO:
//### Client Pool?

type RpcClient struct {
	mu   *sync.Mutex
	conn map[string]*grpc.ClientConn
}

var rrc *RpcClient

func NewRpcClient() *RpcClient {
	if rrc == nil {
		rrc = &RpcClient{
			mu:   &sync.Mutex{},
			conn: make(map[string]*grpc.ClientConn),
		}
	}
	return rrc
}

func (rrc *RpcClient) RPCRequest(rpcAddr string, r *OpRequest) (*OpReply, error) {
	var err error
	rrc.mu.Lock()
	conn := rrc.conn[rpcAddr]
	if conn == nil {
		//Create New Connection
		ServerLogger.Println("New RPC Connect, rpc server addr:", rpcAddr)
		rrc.mu.Unlock()
		conn, err = grpc.Dial(rpcAddr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		rrc.mu.Lock()
		rrc.conn[rpcAddr] = conn
	}

	rrc.mu.Unlock()
	client := NewRaftRpcClient(conn)
	return client.OpRPC(context.Background(), r)
}
