package main

import (
	"./dfslib"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"

	"sync/atomic"
)

// Conurrency write lock
var locker uint32

var rpcConn *rpc.Client

type MetadataObj struct {
	chunkVersionNum [256]int // The index position indicates the chunk #
	owner           string   // stores the client Id to identify the owner
	chunks          [256]dfslib.Chunk
	writtenTo       bool
}

type Client struct {
	clientToServerRpc *rpc.Client
	Files             map[string]dfslib.DFSFileStruct
	Ip                string
	Id                string
	LocalPath         string
	IsConnected       bool
	missedBeats       int
}

type Server struct {
	RegisteredClients map[string]Client
	files             map[string]MetadataObj
	fileNamesSeen     map[string]bool // Keeps track of all the files that has ever been created
}

func (s *Server) RegisterClient(client *dfslib.Client, id *string) error {
	*id = client.LocalPath + strconv.Itoa(rand.Intn(100))

	rpcConn, err := rpc.Dial("tcp", client.Ip)
	dfslib.CheckError("Error in setting up server to client rpc connection in RegisterClient: ", err)
	serverClient := Client{
		clientToServerRpc: rpcConn,
		Files:             client.Files,
		Ip:                client.Ip,
		Id:                *id,
		LocalPath:         client.LocalPath,
		IsConnected:       true,
	}
	s.RegisteredClients[*id] = serverClient

	//handleHeartbeat(client.Ip)
	return nil
}

func (s *Server) UnregisterClient(client *dfslib.Client, isConnected *bool) error {
	c := s.RegisteredClients[client.Id]
	c.clientToServerRpc.Close()
	c.IsConnected = false
	s.RegisteredClients[client.Id] = c
	*isConnected = false
	return nil
}

func (s *Server) IsClientConnected(clientId string, answer *bool) error {
	client := s.RegisteredClients[clientId]
	*answer = client.IsConnected
	return nil
}

func (s *Server) DoesFileExistGlobally(dfsFile *dfslib.DFSFileStruct, exists *bool) error {
	for _, client := range s.RegisteredClients {
		for _, v := range client.Files {
			if v.Name == dfsFile.Name {
				*exists = true
				return nil
			}
		}
	}
	*exists = false
	return nil
}

func (s *Server) IsFileCreated(fname string, ans *bool) error {
	if s.fileNamesSeen[fname] {
		*ans = true
		return nil
	}
	*ans = false
	return nil
}

func (s *Server) AddFileToSeen(fname string, success *bool) error {
	s.fileNamesSeen[fname] = true
	s.files[fname] = MetadataObj{}
	*success = true
	return nil
}

func (s *Server) LinkFileToClient(dfsFile dfslib.DFSFileStruct, success *bool) error {
	c := s.RegisteredClients[dfsFile.Owner]
	c.Files[dfsFile.Name] = dfsFile
	*success = true
	return nil

}

func (s *Server) UpdateChunkVersion(dfsFile *dfslib.DFSFileStruct, success *bool) error {
	// Get the metadata associated with the file
	fileMetadata := s.files[dfsFile.Name]
	fileMetadata.writtenTo = true
	chunkNum := dfsFile.LastChunkWritten
	fileMetadata.chunks[chunkNum] = dfsFile.Chunks[chunkNum]

	// Increment the version number of the appropriate chunk
	fileMetadata.chunkVersionNum[chunkNum]++
	// Set the chunk's owner to be the file that last wrote to it
	fileMetadata.owner = dfsFile.Owner
	s.files[dfsFile.Name] = fileMetadata
	*success = true
	return nil
}

func (s *Server) GetMostUpdatedChunk(args dfslib.FileChunk, answer *dfslib.Chunk) error {
	maxVersion := 0
	maxPossibleVersion := 0
	for _, client := range s.RegisteredClients {
		for fname, _ := range client.Files {
			chunkNum := int(args.ChunkNum)
			if fname == args.FileName {
				fileMetadata := s.files[fname]
				if fileMetadata.chunkVersionNum[chunkNum] > maxPossibleVersion {
					// Figure out the max version ever for a chunk, whether or not
					// the client is still connected
					maxPossibleVersion = fileMetadata.chunkVersionNum[chunkNum]
				}

				// For a given file's metadata object, if the file has chunkNum with
				// a newer version than the current max, and the client is still connected,
				// then we want to return that chunk
				if client.IsConnected && fileMetadata.chunkVersionNum[chunkNum] >= maxVersion {
					*answer = fileMetadata.chunks[chunkNum]
					maxVersion = fileMetadata.chunkVersionNum[chunkNum]
				}
			}
		}
	}
	if maxVersion < maxPossibleVersion {
		return dfslib.ChunkUnavailableError(args.ChunkNum)
	}
	return nil
}

func (s *Server) GetSomeVersionOfFile(fname string, file *dfslib.DFSFileStruct) error {
	// Loop through every client and the list of files each has
	// If one of them has a file that matches the fname, and the client is still connected
	// return it
	for _, client := range s.RegisteredClients {
		for fileName, f := range client.Files {
			if fileName == fname && client.IsConnected {
				*file = f
				return nil
			}
		}
	}
	return nil
}

func (s *Server) HasFileBeenWrittenTo(fname string, answer *bool) error {
	fileMetadataObj := s.files[fname]
	*answer = fileMetadataObj.writtenTo
	return nil
}

func (s *Server) GetWriteLock(fname string, answer *bool) error {
	if !atomic.CompareAndSwapUint32(&locker, 0, 1) {
		*answer = false
		return nil
	}
	*answer = true
	return nil
}

func (s *Server) ReleaseWriteLock(fname string, success *bool) error {
	// Release the lock, if it's relevant (WRITE mode only), when the file is closed
	atomic.StoreUint32(&locker, 0)
	*success = true
	return nil
}

func main() {
	args := os.Args[1:]
	incomingIP := args[0]

	conn, err := net.ResolveTCPAddr("tcp", incomingIP)
	dfslib.CheckError("ResolveTCPAddr failed: ", err)

	incoming, err := net.ListenTCP("tcp", conn)
	dfslib.CheckError("ListenTCP for server failed: ", err)

	server := new(Server)
	server.RegisteredClients = make(map[string]Client)
	server.fileNamesSeen = make(map[string]bool)
	server.files = make(map[string]MetadataObj)
	rpc.Register(server)
	go rpc.Accept(incoming)

	blockForever()

}

// Unused heartbeat code - NOT working
//func (s *Server) Heartbeat(c dfslib.Client, deadClient *string) error {
//	//go func() {
//		for {
//			time.Sleep(time.Second * 2)
//			for _, client := range s.RegisteredClients {
//				if c.Id == client.Id {
//					client.missedBeats = 0
//				} else {
//					client.missedBeats++
//				}
//
//				if client.missedBeats == 2 {
//					*deadClient = client.Id
//				}
//			}
//		}
//	//}()
//	return nil
//}

//func handleHeartbeat(incomingIP string) {
//	heartbeat, err := net.ResolveUDPAddr("udp", incomingIP)
//	dfslib.CheckError("ResolveUDPAddr failed: ", err)
//
//	heartbeatConn, err := net.ListenUDP("udp", heartbeat)
//	dfslib.CheckError("ListenUDP for heartbeat failed: ", err)
//
//	go func() {
//		defer heartbeatConn.Close()
//		readBuf := make([]byte, 100)
//		missedBeats := 0
//		for {
//			n, err := heartbeatConn.Read(readBuf)
//			dfslib.CheckError("Error while receiving the heartbeat from client: ", err)
//
//			if n == 0 {
//				missedBeats++
//			} else {
//				missedBeats = 0
//			}
//
//			// After 3 straight missed beats (6 seconds), assume client is dead
//			if missedBeats == 3 {
//				// close client connection because client died
//				msg := "Close connection"
//				_, err = heartbeatConn.Write([]byte(msg))
//			}
//		}
//	}()
//}

func blockForever() {
	select {}
}
