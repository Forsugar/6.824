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

type TaskState struct {
	BeginSecond int64
	WorkerId    int
}

type Master struct {
	// Your definitions here.
	Filename []string
	NReduce  int

	CurrWorkId int

	UnIssuedMapTasks *BlockQueue
	IssuedMapTasks   *MapSet
	IssuedMapMutex   sync.Mutex

	UnIssuedReduceTasks *BlockQueue
	IssuedReduceTasks   *MapSet
	IssuedReduceMutex   sync.Mutex

	//task states
	MapTasks    []TaskState
	ReduceTasks []TaskState

	//status
	MapDone bool
	AllDone bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GiveMapTask(args *MapTaskArgs, reply *MapTaskReply) error {
	if args.WorkerId == -1 {
		m.CurrWorkId++
		reply.WorkerId = m.CurrWorkId
	} else {
		reply.WorkerId = args.WorkerId
	}
	log.Printf("worker %v asks for a map task\n", reply.WorkerId)

	m.IssuedMapMutex.Lock()
	if m.MapDone {
		m.IssuedMapMutex.Unlock()
		processReplyIfMapDone(reply)
		return nil
	}
	if m.UnIssuedMapTasks.Size() == 0 && m.IssuedMapTasks.Size() == 0 {
		m.IssuedMapMutex.Unlock()
		m.MapDone = true
		processReplyIfMapDone(reply)
		m.preparedAllReduceTasks()
		return nil
	}

	log.Printf("%v unissued map tasks %v issued map tasks at hand\n", m.UnIssuedMapTasks.Size(), m.IssuedMapTasks.Size())
	m.IssuedMapMutex.Unlock() // release lock to allow unissued update
	currTime := getNowTimeSecond()
	ret, error := m.UnIssuedMapTasks.PopBack()
	var fileId int
	if error != nil {
		log.Println("no map task yet, let worker wait...")
		fileId = -1
	} else {
		fileId = ret.(int)
		reply.FileName = m.Filename[fileId]
		m.IssuedMapMutex.Lock()
		m.MapTasks[fileId].BeginSecond = currTime
		m.MapTasks[fileId].WorkerId = reply.WorkerId
		m.IssuedMapTasks.Insert(fileId)
		m.IssuedMapMutex.Unlock()
		log.Printf("giving map task %v on file %v at second %v\n", fileId, reply.FileName, currTime)
	}
	reply.FileId = fileId
	reply.AllDone = false
	reply.NReduce = m.NReduce

	return nil
}

func (m *Master) preparedAllReduceTasks() {
	for i := 0; i < m.NReduce; i++ {
		log.Printf("sending %vth file reduce task to channel\n", i)
		m.UnIssuedReduceTasks.PutFront(i)
	}
}

func getNowTimeSecond() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func processReplyIfMapDone(reply *MapTaskReply) {
	log.Println("all map tasks complete, telling workers to switch to reduce mode")
	reply.FileId = -1
	reply.AllDone = true
}

func (m *Master) JoinMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	fileId := args.FileId
	workerId := args.WorkerId

	// check current time for whether the worker has timed out
	log.Printf("got join map request from worker %v on file %v %v\n", workerId, fileId, m.Filename[fileId])

	m.IssuedMapMutex.Lock()
	currTime := getNowTimeSecond()

	//file map already finish or removed by loop remove thread
	if !m.IssuedMapTasks.Has(fileId) {
		m.IssuedMapMutex.Unlock()
		log.Printf("task abandon or don't exist")
		reply.Accept = false
		return nil
	}
	//not this worker or removed by loop remove thread then send to other worker
	if m.MapTasks[fileId].WorkerId != workerId {
		m.IssuedMapMutex.Unlock()
		log.Printf("task%v don't belong to worker%v", fileId, workerId)
		reply.Accept = false
		return nil
	}
	// time out
	if currTime-m.MapTasks[fileId].BeginSecond > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		m.IssuedMapTasks.Remove(fileId)
		m.UnIssuedMapTasks.PutFront(fileId)
		m.IssuedMapMutex.Unlock()
		reply.Accept = false
	} else {
		log.Println("task within max wait time, accepting...")
		m.IssuedMapTasks.Remove(fileId)
		m.IssuedMapMutex.Unlock()
		reply.Accept = true
	}
	return nil
}

func (m *Master) GiveReduceTask(args *ReduceTaskArgs, reply *ReduceTaskReply) error {
	workerId := args.WorkerId
	m.IssuedReduceMutex.Lock()
	if m.AllDone {
		m.IssuedReduceMutex.Unlock()
		log.Printf("all reduce task already finish")
		processReplyIfAllDone(reply)
		return nil
	}
	if m.UnIssuedReduceTasks.Size() == 0 && m.IssuedReduceTasks.Size() == 0 {
		m.IssuedReduceMutex.Unlock()
		m.AllDone = true
		log.Printf("all reduce task finish")
		processReplyIfAllDone(reply)
		return nil
	}
	log.Printf("%v unissued reduce tasks %v issued reduce tasks at hand\n", m.UnIssuedReduceTasks.Size(), m.IssuedReduceTasks.Size())
	m.IssuedReduceMutex.Unlock()

	currTime := getNowTimeSecond()
	ret, error := m.UnIssuedReduceTasks.PopBack()
	var reduceId int
	if error != nil {
		reduceId = -1
		log.Printf("no map task yet, let worker wait...")
	} else {
		reduceId = ret.(int)

		m.IssuedReduceMutex.Lock()
		m.ReduceTasks[reduceId].BeginSecond = currTime
		m.ReduceTasks[reduceId].WorkerId = workerId
		m.IssuedReduceTasks.Insert(reduceId)
		m.IssuedReduceMutex.Unlock()

		log.Printf("giving reduce task %v at second %v\n", reduceId, currTime)
	}
	reply.ReduceId = reduceId
	reply.NReduce = m.NReduce
	reply.AllDone = false
	reply.FileCount = len(m.Filename)

	return nil
}

func processReplyIfAllDone(reply *ReduceTaskReply) {
	reply.AllDone = true
	reply.ReduceId = -1
}

func (m *Master) JoinReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	workerId := args.WorkerId
	reduceId := args.ReduceId

	// check current time for whether the worker has timed out
	log.Printf("got join reduce request from worker %v reduceID %v\n", workerId, reduceId)

	m.IssuedReduceMutex.Lock()
	currTime := getNowTimeSecond()

	// maybe removed by loopRemove thread because of time out
	if !m.IssuedReduceTasks.Has(reduceId) {
		m.IssuedReduceMutex.Unlock()
		reply.Accept = false
		log.Printf("task abandon or don't exist")
		return nil
	}
	// maybe removed by loopRemove thread and then this task is sent to other worker
	if m.ReduceTasks[reduceId].WorkerId != workerId {
		m.IssuedReduceMutex.Unlock()
		reply.Accept = false
		log.Printf("task%v don't belong to worker%v", reduceId, workerId)
		return nil
	}

	//time out
	if currTime-m.ReduceTasks[reduceId].BeginSecond > maxTaskTime {
		log.Println("task exceeds max wait time, abadoning...")
		m.IssuedReduceTasks.Remove(reduceId)
		m.UnIssuedReduceTasks.PutFront(reduceId)
		reply.Accept = false
	} else {
		log.Println("task within max wait time, accepting...")
		m.IssuedReduceTasks.Remove(reduceId)
		reply.Accept = true
	}
	m.IssuedReduceMutex.Unlock()

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
	if m.AllDone {
		ret = true
		log.Println("asked whether i am done, replying yes...")
	} else {
		log.Println("asked whether i am done, replying no...")
	}

	return ret
}

func (m *Master) loopRemoveTimeOutTasks() {
	for true {
		time.Sleep(2 * 1000 * time.Millisecond)
		m.removeTimeOutTasks()
	}
}

func (m *Master) removeTimeOutTasks() {
	//stop the world
	log.Println("removing timeout tasks...")

	m.IssuedMapMutex.Lock()
	m.IssuedMapTasks.RemoveTimeOutTasksFromMapSet(m.UnIssuedMapTasks, m.MapTasks, "map")
	//m.IssuedMapTasks.RemoveTimeoutMapTasks(m.MapTasks, m.UnIssuedMapTasks)
	m.IssuedMapMutex.Unlock()

	m.IssuedReduceMutex.Lock()
	m.IssuedReduceTasks.RemoveTimeOutTasksFromMapSet(m.UnIssuedReduceTasks, m.ReduceTasks, "reduce")
	//m.IssuedReduceTasks.RemoveTimeoutReduceTasks(m.ReduceTasks, m.UnIssuedReduceTasks)
	m.IssuedReduceMutex.Unlock()
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

	m.CurrWorkId = -1
	m.Filename = files
	m.NReduce = nReduce

	m.UnIssuedMapTasks = NewBlockQueue()
	m.IssuedMapTasks = NewMapSet()

	m.UnIssuedReduceTasks = NewBlockQueue()
	m.IssuedReduceTasks = NewMapSet() //粗心！

	m.MapTasks = make([]TaskState, len(files))
	m.ReduceTasks = make([]TaskState, nReduce)

	m.MapDone = false
	m.AllDone = false

	m.server()
	log.Println("listening started...")

	// starts a thread that abandons timeout tasks
	go m.loopRemoveTimeOutTasks()

	// all are unissued map tasks
	// send to channel after everything else initializes
	log.Printf("file count %d\n", len(files))
	for i := 0; i < len(files); i++ {
		log.Printf("sending %vth file map task to channel\n", i)
		m.UnIssuedMapTasks.PutFront(i)
	}
	return &m
}
