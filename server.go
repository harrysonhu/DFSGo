package main

import (
	//"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"./dfslib"
	"math/rand"
)

type Listener int

func (l *Listener) RegisterClient(client *dfslib.Client, clientId *int) error {
	*clientId = rand.Intn(1000)
	client.IsConnected = true
	return nil
}

func (l *Listener) UnregisterClient(client *dfslib.Client, isConnected *bool) error {
	*isConnected = false
	return nil
}

func main() {
	args := os.Args[1:]
	incomingIP := args[0]

	conn, err := net.ResolveTCPAddr("tcp", incomingIP)
	dfslib.CheckError("ResolveTCPAddr failed: ", err)

	incoming, err := net.ListenTCP("tcp", conn)
	dfslib.CheckError("ListenTCP for server failed: ", err)

	handleHeartbeat(incomingIP)

	listener := new(Listener)
	rpc.Register(listener)
	rpc.Accept(incoming)

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
			}
		}
	}()
}

