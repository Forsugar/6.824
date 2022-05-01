package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTaskState struct {
}

type ReduceTaskState struct {
}

type Master struct {
	// Your definitions here.
	filename []string
	nReduce  int

	currWorkId int

	unIssuedMapTasks *BlockQueue
	issuedMapTasks   *MapSet
	issuedMapMutex   sync.Mutex

	unIssuedReduceTasks *BlockQueue
	issuedReduceTasks   *MapSet
	issuedReduceMutex   sync.Mutex

	//task states
	mapTasks    []MapTaskState
	reduceTasks []ReduceTaskState

	//status
	mapDone bool
	allDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	log.SetPrefix("coordinator: ")
	log.Println("making coordinator")

	m.currWorkId = -1
	m.filename = files
	m.nReduce = nReduce

	m.unIssuedReduceTasks = NewBlockQueue()
	m.issuedMapTasks = NewMapSet()

	m.unIssuedReduceTasks = NewBlockQueue()
	m.issuedMapTasks = NewMapSet()

	m.mapTasks = make([]MapTaskState, len(files))
	m.reduceTasks = make([]ReduceTaskState, nReduce)

	m.mapDone = false
	m.allDone = false

	m.server()
	return &m
}
