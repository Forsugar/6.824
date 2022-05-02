package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const maxTaskTime = 10 // seconds

type MapTaskState struct {
	beginSecond int64
	workId      int
	//fileId      int
}

type ReduceTaskState struct {
	beginSecond int64
	workId      int
	//reduceId    int
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

func (m *Master) giveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.workerId == -1 {
		m.currWorkId++
		reply.workerId = m.currWorkId
	} else {
		reply.workerId = args.workerId
	}
	log.Printf("worker %v asks for a map task\n", reply.workerId)

	m.issuedMapMutex.Lock()
	if m.mapDone {
		m.issuedMapMutex.Unlock()
		mapDoneProcess(reply)
		return nil
	}
	if m.unIssuedMapTasks.Size() == 0 && m.issuedMapTasks.Size() == 0 {
		m.issuedMapMutex.Unlock()
		m.mapDone = true
		mapDoneProcess(reply)
		m.preparedAllReduceTasks()
		return nil
	}

	log.Printf("%v unissued map tasks %v issued map tasks at hand\n", m.unIssuedMapTasks.Size(), m.issuedMapTasks.Size())
	m.issuedMapMutex.Unlock() // release lock to allow unissued update
	currTime := getNowTimeSecond()
	ret, error := m.unIssuedMapTasks.PopBack()
	var fileId int
	if error != nil {
		log.Println("no map task yet, let worker wait...")
		fileId = -1
	} else {
		fileId = ret.(int)
		reply.fileName = m.filename[fileId]
		m.issuedMapMutex.Lock()
		m.mapTasks[fileId].beginSecond = currTime
		m.mapTasks[fileId].workId = reply.workerId
		m.issuedMapTasks.Insert(fileId)
		m.issuedMapMutex.Unlock()
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.fileName, currTime)
	}
	reply.fileId = fileId
	reply.allDone = false
	reply.nReduce = m.nReduce

	return nil
}

func (m *Master) preparedAllReduceTasks() {
	for i := 0; i < m.nReduce; i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		m.unIssuedMapTasks.PutFront(i)
	}
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func mapDoneProcess(reply *MapTaskReply) {
	log.Println("all map tasks complete, telling workers to switch to reduce mode")
	reply.fileId = -1
	reply.allDone = true
}

func (m *Master) joinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	fileId := args.fileId
	workerId := args.workerId

	// check current time for whether the worker has timed out
	log.Printf("got join map request from worker %v on file %v %v\n", workerId, fileId, m.filename[fileId])

	m.issuedMapMutex.Lock()
	currTime := getNowTimeSecond()

	//file map already finish
	if !m.issuedMapTasks.Has(fileId) {
		m.issuedMapMutex.Unlock()
		log.Printf("task abandon or don't exist")
		reply.accept = false
		return nil
	}
	//not this worker
	if m.mapTasks[fileId].workId != workerId {
		m.issuedMapMutex.Unlock()
		log.Printf("task%v don't belong to worker%v", fileId, workerId)
		reply.accept = false
		return nil
	}
	// time out
	if currTime-m.mapTasks[fileId].beginSecond > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		//m.issuedMapTasks.Remove(fileId)
		m.unIssuedMapTasks.PutFront(fileId)
		m.issuedMapMutex.Unlock()
		reply.accept = false
	} else {
		log.Println("task within max wait time, accepting...")
		m.issuedMapTasks.Remove(fileId)
		m.issuedMapMutex.Unlock()
		reply.accept = true
	}
	return nil
}

func (m *Master) giveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	workerId := args.workerId
	m.issuedReduceMutex.Lock()
	if m.allDone {
		m.issuedReduceMutex.Unlock()
		log.Printf("all reduce task already finish")
		allDoneProcess(reply)
		return nil
	}
	if m.unIssuedReduceTasks.Size() == 0 && m.issuedReduceTasks.Size() == 0 {
		m.issuedReduceMutex.Unlock()
		m.allDone = true
		log.Printf("all reduce task finish")
		allDoneProcess(reply)
		return nil
	}
	log.Printf("%v unissued reduce tasks %v issued reduce tasks at hand\n", m.unIssuedReduceTasks.Size(), m.issuedReduceTasks.Size())
	m.issuedReduceMutex.Unlock()

	currTime := getNowTimeSecond()
	ret, error := m.unIssuedReduceTasks.PopBack()
	var reduceId int
	if error != nil {
		reduceId = -1
		log.Printf("no map task yet, let worker wait...")
	} else {
		reduceId = ret.(int)

		m.issuedReduceMutex.Lock()
		m.reduceTasks[reduceId].beginSecond = currTime
		m.reduceTasks[reduceId].workId = workerId
		m.issuedReduceTasks.Insert(reduceId)
		m.issuedReduceMutex.Unlock()
		log.Printf("giving reduce task %v at second %v\n", reduceId, currTime)
	}
	reply.reduceId = reduceId
	reply.nReduce = m.nReduce
	reply.allDone = false
	reply.fileCount = len(m.filename)

	return nil
}

func allDoneProcess(reply *ReduceTaskReply) {
	reply.allDone = true
	reply.reduceId = -1
}

func (m *Master) joinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	workerId := args.workerId
	reduceId := args.reduceId

	// check current time for whether the worker has timed out
	log.Printf("got join reduce request from worker %v reduceID %v\n", workerId, reduceId)

	m.issuedReduceMutex.Lock()
	currTime := getNowTimeSecond()

	if !m.issuedMapTasks.Has(reduceId) {
		m.issuedReduceMutex.Unlock()
		reply.accept = false
		log.Printf("task abandon or don't exist")
		return nil
	}
	if m.reduceTasks[reduceId].workId != workerId {
		m.issuedReduceMutex.Unlock()
		reply.accept = false
		log.Printf("task%v don't belong to worker%v", reduceId, workerId)
		return nil
	}

	if currTime-m.reduceTasks[reduceId].beginSecond > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		m.unIssuedReduceTasks.PutFront(reduceId)
		reply.accept = false
	} else {
		log.Println("task exceeds max wait time, abadoning...")
		m.issuedMapTasks.Remove(reduceId)
		reply.accept = true
	}
	m.issuedReduceMutex.Unlock()

	return nil
}

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

func (m *Master) loopRemoveTimeOutMap() {

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

	m.unIssuedMapTasks = NewBlockQueue()
	m.issuedMapTasks = NewMapSet()

	m.unIssuedReduceTasks = NewBlockQueue()
	m.issuedMapTasks = NewMapSet()

	m.mapTasks = make([]MapTaskState, len(files))
	m.reduceTasks = make([]ReduceTaskState, nReduce)

	m.mapDone = false
	m.allDone = false

	m.server()
	log.Println("listening started...")

	// starts a thread that abandons timeout tasks
	go m.loopRemoveTimeOutMap()

	// all are unissued map tasks
	// send to channel after everything else initializes
	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		m.unIssuedReduceTasks.PutFront(i)
	}
	return &m
}
