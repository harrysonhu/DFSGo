package main

import (
	//"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"./dfslib"
	"math/rand"
	"strconv"
)

var rpcConn *rpc.Client

type Client struct {
	clientToServerRpc *rpc.Client
	Files map[string]dfslib.DFSFileStruct
	Ip string
	Id string
	LocalPath string
	IsConnected bool
}

type Server struct {
	RegisteredClients map[string]Client
}

func (s *Server) RegisterClient(client *dfslib.Client, id *string) error {
	*id = client.LocalPath + strconv.Itoa(rand.Intn(100))

	rpcConn, err := rpc.Dial("tcp", client.Ip)
	dfslib.CheckError("Error in setting up server to client rpc connection in RegisterClient: ", err)
	client.IsConnected = true
	serverClient := Client{
		clientToServerRpc: rpcConn,
		Files: client.Files,
		Ip: client.Ip,
		Id: *id,
		LocalPath: client.LocalPath,
		IsConnected: true,
	}
	s.RegisteredClients = make(map[string]Client)
	s.RegisteredClients[*id] = serverClient
	return nil
}

func (s *Server) UnregisterClient(client *dfslib.Client, isConnected *bool) error {
	c := s.RegisteredClients[client.Id]
	c.clientToServerRpc.Close()
	c.IsConnected = false
	*isConnected = false
	return nil
}

func (s *Server) DoesFileExist(dfsFile *dfslib.DFSFileStruct, exists *bool) error {
	for _, client := range s.RegisteredClients {
		for _, v := range client.Files {
			if v.Name == dfsFile.Name && client.IsConnected {
				*exists = true
				return nil
			}
		}
	}
	*exists = false
	return nil
}

func main() {
	args := os.Args[1:]
	incomingIP := args[0]

	conn, err := net.ResolveTCPAddr("tcp", incomingIP)
	dfslib.CheckError("ResolveTCPAddr failed: ", err)

	incoming, err := net.ListenTCP("tcp", conn)
	dfslib.CheckError("ListenTCP for server failed: ", err)

	//handleHeartbeat(incomingIP)

	server := new(Server)
	rpc.Register(server)
	go rpc.Accept(incoming)

	blockForever()

}

func handleHeartbeat(incomingIP string) {
	heartbeat, err := net.ResolveUDPAddr("udp", incomingIP)
	dfslib.CheckError("ResolveUDPAddr failed: ", err)

	heartbeatConn, err := net.ListenUDP("udp", heartbeat)
	dfslib.CheckError("ListenUDP for heartbeat failed: ", err)

	go func() {
		defer heartbeatConn.Close()
		readBuf := make([]byte, 100)
		missedBeats := 0
		for {
			n, err := heartbeatConn.Read(readBuf)
			dfslib.CheckError("Error while receiving the heartbeat from client: ", err)

			if n == 0 {
				missedBeats++
			} else {
				missedBeats = 0
			}

			// After 3 straight missed beats (6 seconds), assume client is dead
			if missedBeats == 3 {
				// close client connection because client died
				msg := "Close connection"
				_, err = heartbeatConn.Write([]byte(msg))
			}
		}
	}()
}

func blockForever() {
	select {}
}

