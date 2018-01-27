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

type MetadataObj struct {
	chunkVersionNum [256]int					// The index position indicates the chunk #
	owner string								// stores the client Id to identify the owner
}

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
	files map[string]MetadataObj
	fileNamesSeen map[string]bool               // Keeps track of all the files that has ever been created
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

func (s *Server) DoesFileExistGlobally(dfsFile *dfslib.DFSFileStruct, exists *bool) error {
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
	chunkNum := dfsFile.LastChunkWritten

	// Increment the version number of the appropriate chunk
	fileMetadata.chunkVersionNum[chunkNum]++
	// Set the chunk's owner to be the file that last wrote to it
	fileMetadata.owner = dfsFile.Owner
	s.files[dfsFile.Name] = fileMetadata
	*success = true
	return nil
}

// This function loops through every file to find the latest versions of each chunk,
// creates a file with these chunks written into it, and returns it back to the client
func (s *Server) GetMostUpdatedFile(c dfslib.Client, answer *os.File) error {
	var file os.File
	var chunkVersions [256]int
	// Loop through all files on the server and for each file, loop through all 256 chunks.
	// For each chunk, check the version against the new file's version.
	// If the version is newer, overwrite the new file's chunk with the most up to date version
	for fname, obj := range s.files {
		for i := 0; i < 256; i++ {
			if obj.chunkVersionNum[i] > chunkVersions[i] {
				owner := s.RegisteredClients[obj.owner]

				// Check if owner is even connected first before fetching
				if owner.IsConnected {
					fileWithLatestChunkVersion := owner.Files[fname]
					var chunk dfslib.Chunk
					// Read the chunk from the file that owns the latest version of it
					fileWithLatestChunkVersion.Read(uint8(i), &chunk)

					offset := int64(i * 32)
					b := chunk[:]
					// Write the latest chunk into the file at the right offset
					_, err := file.WriteAt(b, offset)
					dfslib.CheckError("Error in GetMostUpdatedFile: ", err)

					chunkVersions[i] = obj.chunkVersionNum[i]
				}
			}
		}
	}

	*answer = file
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
	server.RegisteredClients = make(map[string]Client)
	server.fileNamesSeen = make(map[string]bool)
	server.files = make(map[string]MetadataObj)
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

