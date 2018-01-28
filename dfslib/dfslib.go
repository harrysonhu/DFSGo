/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import "fmt"
import "net/rpc"
import "os"
import (
    "net"
    "time"
    "strings"
)

var rServerConn *rpc.Client
var globalServerAddr string

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
    // Read mode.
    READ FileMode = iota

    // Read/Write mode.
    WRITE

    // Disconnected read mode.
    DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
    return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
    return fmt.Sprintf("DFS: Latest verson of chunk [%s] unavailable", string(e))
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
    return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
    return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
    return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
    return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
    return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
    return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
    // Reads chunk number chunkNum into storage pointed to by
    // chunk. Returns a non-nil error if the read was unsuccessful.
    //
    // Can return the following errors:
    // - DisconnectedError (in READ,WRITE modes)
    // - ChunkUnavailableError (in READ,WRITE modes)
    Read(chunkNum uint8, chunk *Chunk) (err error)

    // Writes chunk number chunkNum from storage pointed to by
    // chunk. Returns a non-nil error if the write was unsuccessful.
    //
    // Can return the following errors:
    // - BadFileModeError (in READ,DREAD modes)
    // - DisconnectedError (in WRITE mode)
    Write(chunkNum uint8, chunk *Chunk) (err error)

    // Closes the file/cleans up. Can return the following errors:
    // - DisconnectedError
    Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
    // Check if a file with filename fname exists locally (i.e.,
    // available for DREAD reads).
    //
    // Can return the following errors:
    // - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
    LocalFileExists(fname string) (exists bool, err error)

    // Check if a file with filename fname exists globally.
    //
    // Can return the following errors:
    // - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
    // - DisconnectedError
    GlobalFileExists(fname string) (exists bool, err error)

    // Opens a filename with name fname using mode. Creates the file
    // in READ/WRITE modes if it does not exist. Returns a handle to
    // the file through which other operations on this file can be
    // made.
    //
    // Can return the following errors:
    // - OpenWriteConflictError (in WRITE mode)
    // - DisconnectedError (in READ,WRITE modes)
    // - FileUnavailableError (in READ,WRITE modes)
    // - FileDoesNotExistError (in DREAD mode)
    // - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
    Open(fname string, mode FileMode) (f DFSFile, err error)

    // Disconnects from the server. Can return the following errors:
    // - DisconnectedError
    UMountDFS() (err error)
}

type Client struct {
    clientToServerRpc *rpc.Client
    Files map[string]DFSFileStruct
    Ip string
    Id string
    LocalPath string
    IsConnected bool
}

type DFSFileStruct struct {
    connection *rpc.Client
    Owner string                     // Owner of the file is the client Id
    Name string
    file os.File
    mode FileMode
    LastChunkWritten int
    Chunks [256]Chunk
}

func (dfs DFSFileStruct) Read(chunkNum uint8, chunk *Chunk) (err error) {
    if chunkNum < 0 || chunkNum > 255 {
        return ChunkUnavailableError(chunkNum)
    }
    // Can't read in READ mode if the client is disconnected
    if (dfs.mode == READ || dfs.mode == WRITE) && dfs.isClientConnected(dfs.Owner) == false {
        return DisconnectedError(globalServerAddr)
    }
    readBuf := make([]byte, 32, 32)
    offset := int64(chunkNum * 32)
    _, err = dfs.file.ReadAt(readBuf, offset)
    CheckError("Error in reading a chunk of a file: ", err)
    copy(chunk[:], readBuf[:])
    return nil
}

func (dfs DFSFileStruct) Write(chunkNum uint8, chunk *Chunk) (err error) {
    if dfs.mode == READ || dfs.mode == DREAD {
        return BadFileModeError(dfs.mode)
    }
    // Can't write in WRITE mode if the client is disconnected
    if (dfs.mode == READ || dfs.mode == WRITE) && dfs.isClientConnected(dfs.Owner) == false {
        return DisconnectedError(globalServerAddr)
    }

    offset := int64(chunkNum * 32)
    b := chunk[:]
    n, err := dfs.file.WriteAt(b, offset)
    dfs.Chunks[chunkNum] = *chunk
    CheckError("Error in writing to a file: ", err)
    fmt.Printf("wrote %d bytes\n", n)
    dfs.file.Sync()

    dfs.LastChunkWritten = int(chunkNum)
    var success bool
    dfs.connection.Call("Server.UpdateChunkVersion", dfs, &success)
    return nil
}

func (dfs DFSFileStruct) Close() (err error) {
    if (dfs.mode == READ || dfs.mode == WRITE) && dfs.isClientConnected(dfs.Owner) == false {
        return DisconnectedError(globalServerAddr)
    }
    if dfs.mode == WRITE {
        var success bool
        dfs.connection.Call("Server.ReleaseWriteLock", dfs.Name, &success)
    }
    err = dfs.file.Close()
    CheckError("Error in closing the file: ", err)

    return nil
}

func (c Client) LocalFileExists(fname string) (exists bool, err error) {
    if isBadFileName(fname) {
        return false, BadFilenameError(fname) 
    }

    // Loop through all the entries in a Client's files map
    for k, _ := range c.Files {
        if k == fname {
            return true, nil
        }
    }
    return false, nil
}

func (c Client) GlobalFileExists(fname string) (exists bool, err error) {
    if isBadFileName(fname) {
        return false, BadFilenameError(fname) 
    }
    // if client is not even connected, there's no need to check if file exists globally
    if c.IsConnected == false {
        return false, DisconnectedError(globalServerAddr)
    }

    dfsFile := DFSFileStruct{
        Name: fname,
    }
    err = c.clientToServerRpc.Call("Server.DoesFileExistGlobally", dfsFile, &exists)
    CheckError("Error in checking if file exists globally: ", err)

    return exists, err
}

 func (c Client) Open(fname string, mode FileMode) (f DFSFile, err error) {
     if isBadFileName(fname) {
         return nil, BadFilenameError(fname)
     }

     // Can't operate on a file that is "owned" by a client that is disconnected
     if (mode == READ || mode == WRITE) && c.IsConnected == false {
         return nil, DisconnectedError(globalServerAddr)
     }
     // If the mode is WRITE, the file must acquire the lock before it can even open
     // Throw error if it cacnnot acquire the lock
     if mode == WRITE {
         var success bool
         c.clientToServerRpc.Call("Server.GetWriteLock", fname, &success)
         if !success {
             return nil, OpenWriteConflictError(fname)
         }
     }
     fileExistsLocally, _ := c.LocalFileExists(fname)
     fileExistsGlobally, _ := c.GlobalFileExists(fname)
     if fileExistsLocally {
         if mode == READ {
             file, err := os.OpenFile(fname, os.O_RDONLY, 0666)
             CheckError("Error in opening the file for READ: ", err)
             f := c.Files[fname]
             f.file = *file
         } else if mode == WRITE {
             file, err := os.OpenFile(fname, os.O_RDWR, 0666)
             CheckError("Error in opening the file for WRITE: ", err)
             f := c.Files[fname]
             f.file = *file
         } else if mode == DREAD {
             file, err := os.OpenFile(fname, os.O_RDONLY, 0666)
             CheckError("Error in opening the file for DREAD: ", err)
             f := c.Files[fname]
             f.file = *file
         }
         return f, nil
     }

     var chunkMap map[int]Chunk
     if fileExistsGlobally {
         c.clientToServerRpc.Call("Server.GetMostUpdatedFile", c, &chunkMap)
         var writtenTo bool
         c.clientToServerRpc.Call("Server.HasFileBeenWrittenTo", fname, &writtenTo)
         // Check if file is trivial (each chunk version in chunkMap is 0 if len(chunkMap) is 0)
         if len(chunkMap) == 0 {
             if writtenTo {
                 // The file exists globally, is not trivial,
                 // but the server is unable to download a copy of it,
                 // so return FileUnavailableError
                 return nil, FileUnavailableError(fname)
             } else {
                 f = createFile(c, fname, mode)
                 return f, nil
             }
         }
         fname = fname + ".dfs"
		 file, err := os.OpenFile(fname, os.O_RDWR, 0666)
		 CheckError("Error in downloading a non-trivial file from server and opening it: ", err)
         for chunkNum, chunk := range chunkMap {
             offset := chunkNum * 32
             b := chunk[:]
             file.WriteAt(b, int64(offset))
         }
         // Get the DFSFileStruct from the map
         f := c.Files[fname]
         f.connection = c.clientToServerRpc
         f.Owner = c.Id
         f.Name = fname
         f.file = *file
         f.mode = mode
         return f, nil
     }
     f = createFile(c, fname, mode)
     return f, nil
 }

func (c Client) UMountDFS() (err error) {
    // Already disconnected, so throw error if client tries to disconnect again
    if c.IsConnected == false {
        return DisconnectedError(globalServerAddr)
    }
    var isConnected bool
    c.clientToServerRpc.Call("Server.UnregisterClient", c, &isConnected)
    c.IsConnected = isConnected
    c.clientToServerRpc.Close()
    // Loop through every file a client has and close it
    for _, dfsFile := range c.Files {
        dfsFile.file.Close()
    }

    return nil
}

func createFile (c Client, fname string, mode FileMode) DFSFile {
    // If file doesn't exist globally or locally, create it here
    file, err := os.Create(fname + ".dfs")

    array := make([]byte, 8192, 8192)
    initialWrite := array[:]
    n, err := file.Write(initialWrite)
    fmt.Printf("wrote %d bytes\n", n)

    CheckError("Error in creating the file: ", err)
    dfsFileStruct := DFSFileStruct{
        connection: c.clientToServerRpc,
        Owner: c.Id,
        Name: fname,
        file: *file,
        mode: mode,
    }
    c.Files[fname] = dfsFileStruct
    // Tell the server which files have already been created
    var addSuccessful bool
    c.clientToServerRpc.Call("Server.AddFileToSeen", fname, &addSuccessful)
    var linkSuccessful bool
    c.clientToServerRpc.Call("Server.LinkFileToClient", dfsFileStruct, &linkSuccessful)
    return c.Files[fname]
}

func isBadFileName(fname string) bool {
    const alphaNumeric = "abcdefghijklmnopqrstuvwxyz0123456789"
    if len(fname) < 1 || len(fname) > 16 {
        return true
    }

    for _, char := range fname {
        if !strings.Contains(alphaNumeric, string(char)) {
            return true
        }
    }
    return false
}

func (dfs DFSFileStruct) isClientConnected(clientId string) bool {
    var isConnected bool
    dfs.connection.Call("Server.IsClientConnected", clientId, &isConnected)
	return isConnected;
}

func GoBeat(sAddr string, c Client) {
    go func() {
        for {
            Beat(sAddr, "Hello")
            // Beat every 2 seconds
            time.Sleep(time.Second * 2)
        }
    }()

    // Listen to server response.. if server responds close, then close the
    // client that is associated with this GoBeat (passed in parameters)
    heartbeat, err := net.ResolveUDPAddr("udp", sAddr)
    CheckError("ResolveUDPAddr failed: ", err)

    heartbeatConn, err := net.ListenUDP("udp", heartbeat)
    CheckError("ListenUDP for heartbeat failed: ", err)
    readBuf := make([]byte, 100)
    n, err := heartbeatConn.Read(readBuf)
    CheckError("Reading heartbeat message from server failed: ", err)
    if n > 0 {
        c.UMountDFS()
    }
}

func Beat(sAddr string, msg string) {
    conn, err := net.DialTimeout("udp", sAddr, time.Second * 2)
    CheckError("Error in setting up heartbeat connection: ", err)
    // Close connection after every beat call
    defer conn.Close()
    
    fmt.Println("Heartbeat debug msg: ", msg)
    _, err = conn.Write([]byte(msg))

    return
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
    globalServerAddr = serverAddr
    if _, err := os.Stat("." + localPath); err != nil {
        // localPath does not exist
        if os.IsNotExist(err) {
            return nil, LocalPathError(localPath)
        }
    }

    localIP = localIP + ":0"
    client := Client{
        Files: make(map[string]DFSFileStruct),
        LocalPath: localPath,
    }

    conn, err := net.ResolveTCPAddr("tcp", localIP)
    CheckError("Error in resolving serverAddr in MountDFS: ", err)
    rpcConn, err := net.ListenTCP("tcp", conn)
    CheckError("Error in setting up server-client rpc in MountDFS: ", err)
    clientIP := rpcConn.Addr()
    client.Ip = clientIP.String()

    rpc.Register(client)
    go rpc.Accept(rpcConn)

    // Connect to server
    rServerConn, err := rpc.Dial("tcp", serverAddr)
    CheckError("Dialing the server: ", err)
    client.clientToServerRpc = rServerConn

    var id string
    err = rServerConn.Call("Server.RegisterClient", client, &id)
    CheckError("RegisterClient error: ", err)
    client.Id = id
    client.IsConnected = true

    // start UDP heartbeat for client
    //GoBeat(serverAddr, client)

    return client, nil
}

func CheckError(msg string, err error) {
    if err != nil {
        fmt.Println(msg, err)
        os.Exit(-1)
    }
}