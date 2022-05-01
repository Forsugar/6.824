package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapTaskArgs struct {
	workerId int
}

type MapTaskReply struct {
	//the file that will process by this worker
	fileName string
	//id for each file, -1 means no more file
	fileId   int
	nReduce  int
	workerId int
	// if not , and fileId is -1, the worker waits
	allDone bool
}

type MapTaskJoinArgs struct {
	fileId   int
	workerId int
}

type MapTaskJoinReply struct {
	accept bool
}

type AWorker struct {
	mapf    func(string, string) []KeyValue //方法要带返回值
	reducef func(string, []string) string

	// true on map   , false on reduce
	mapOrReduce bool
	//if true , map and reduce finished
	allDone  bool
	workerId int
}

func (worker *AWorker) logPrintf(format string, vars ...interface{}) {
	log.Printf("worker %d: "+format, worker.workerId, vars)
}

func (worker *AWorker) process() {
	if worker.allDone {

	}
	if worker.mapOrReduce {
		reply := worker.askForMapTask()
		if reply == nil {
			worker.mapOrReduce = false
		} else {
			if reply.fileId == -1 {
				// no map task to do now
			} else {
				worker.executeMapTask(reply)
			}
		}
	}
	if !worker.mapOrReduce {
		reply := worker.askForReduceTask()
		if reply == nil {
			worker.allDone = true
		} else {
			if reply.reduceId == -1 {
				// no reduce task to do now
			} else {
				worker.executeReduceTask(reply)
			}
		}
	}
}

func (worker *AWorker) askForMapTask() *MapTaskReply {
	args := MapTaskArgs{}
	args.workerId = worker.workerId
	reply := MapTaskReply{}

	worker.logPrintf("requesting for map task...\n")
	call("Coordinator.giveMapTask", &args, &reply)

	//obstain a workerId
	worker.workerId = reply.workerId

	if reply.fileId == -1 {
		if reply.allDone {
			worker.logPrintf("no more map tasks, switch to reduce mode\n")
		} else {
			return &reply
		}
	}
	worker.logPrintf("got map task on file %v %v\n", reply.fileId, reply.fileName)
	//given a task
	return &reply
}

func (worker *AWorker) executeMapTask(reply *MapTaskReply) {
	intermediate := makeIntermediateFromFile(reply.fileName, worker.mapf)
	worker.logPrintf("writing map results to file\n")
	worker.writeToFiles(reply.fileId, reply.nReduce, intermediate)
	worker.joinMapTask(reply.fileId)
}

func (worker *AWorker) writeToFiles(fileId int, nReduce int, intermediate []KeyValue) {
	kvToWrite := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvToWrite[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := getIndexByKeyAndReduce(kv.Key, nReduce)
		kvToWrite[index] = append(kvToWrite[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		tempfile, error := ioutil.TempFile(".", "mrtemp")
		if error != nil {
			log.Fatal("create temp file failed\n")
		}
		en := json.NewEncoder(tempfile)
		for _, kv := range kvToWrite[i] {
			error = en.Encode(&kv) //这样写的不是地址嘛
			if error != nil {
				worker.logPrintf("encode error!\n")
			}
		}
		outName := fmt.Sprintf("mr-%v-%v", fileId, i)
		error = os.Rename(tempfile.Name(), outName)
		if error != nil {
			worker.logPrintf("rename tempfile failed for $v\n", outName)
		}
	}
}

func (worker *AWorker) joinMapTask(fileId int) {
	args := MapTaskJoinArgs{}
	args.workerId = worker.workerId
	args.fileId = fileId
	reply := MapTaskJoinReply{}

	worker.logPrintf("begin to join")
	call("Coordinator.joinMapTask", &args, &reply)

	if reply.accept {
		worker.logPrintf("accepted !\n")
	} else {
		worker.logPrintf("not accepted !\n")
	}
}

func getIndexByKeyAndReduce(key string, n int) int {
	return ihash(key) % n
}

func makeIntermediateFromFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, error := os.Open(filename)
	if error != nil {
		log.Fatalf("Can't open path %s\n", filename)
	}

	content, error := ioutil.ReadAll(file)
	if error != nil {
		log.Fatalf("Can't read file %s\n", content)
	}

	file.Close()
	kv := mapf(filename, string(content))
	return kv
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type ReduceTaskArgs struct {
	workerId int
}

type ReduceTaskReply struct {
	// index of reduce task
	reduceId  int
	nReduce   int
	fileCount int
	allDone   bool
}

type ReduceTaskJoinArgs struct {
	workerId int
	reduceId int
}

type ReduceTaskJoinReply struct {
	accept bool
}

func (worker *AWorker) askForReduceTask() *ReduceTaskReply {
	args := ReduceTaskArgs{}
	args.workerId = worker.workerId
	reply := ReduceTaskReply{}

	worker.logPrintf("requesting for map task...\n")
	call("Coordinator.giveReduceTask", &args, &reply)

	// refused to give a task
	if reply.reduceId == -1 {
		if reply.allDone {
			worker.logPrintf("no more reduce tasks, try to terminate worker\n")
			return nil
		} else {
			return &reply
		}
	}
	worker.logPrintf("got reduce task on %vth cluster", reply.reduceId)

	//give a reduce task
	return &reply
}

func (worker *AWorker) executeReduceTask(reply *ReduceTaskReply) {
	intermediate := make([]KeyValue, 0)
	for i := 0; i < reply.fileCount; i++ {
		intermediate = append(intermediate, readIntermediate(i, reply.reduceId)...) //三个点的作用：解包切片
	}
	worker.logPrintf("total intermediate count %v\n", len(intermediate))

	outName := fmt.Sprintf("mr-out-%d", reply.reduceId)
	file, error := os.Create(outName)
	if error != nil {
		worker.logPrintf("reduce task create outfile failed\n")
	}
	reduceKVSlice(intermediate, worker.reducef, file)
	file.Close()

	worker.joinReduceTask(reply.reduceId)
}

func (worker *AWorker) joinReduceTask(reduceId int) {
	args := ReduceTaskJoinArgs{}
	args.workerId = worker.workerId
	args.reduceId = reduceId
	reply := ReduceTaskJoinReply{}

	worker.logPrintf("reduce begin join")
	call("Coordinator.joinReduceTask", &args, &reply)

	if reply.accept {
		worker.logPrintf("accepted!")
	} else {
		worker.logPrintf("not accept!")
	}
}

func reduceKVSlice(intermediate []KeyValue, reducef func(string, []string) string, file *os.File) {
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func readIntermediate(fileId int, reduceId int) []KeyValue {
	fileName := fmt.Sprintf("mr-%v-%v", fileId, reduceId)
	file, error := os.Open(fileName)
	if error != nil {
		log.Fatalf("reduce read mr-%v-%v failed \n", fileId, reduceId)
	}
	de := json.NewDecoder(file)
	kvArrOut := make([]KeyValue, 0)
	for { //for循环内部存疑
		var kv KeyValue
		error = de.Decode(&kv) // & ?
		if error != nil {
			break
		}
		kvArrOut = append(kvArrOut, kv)
	}
	file.Close()
	return kvArrOut
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := AWorker{}
	worker.mapf = mapf
	worker.reducef = reducef
	worker.mapOrReduce = true
	worker.allDone = false
	worker.workerId = -1
	worker.logPrintf("initialized!\n")

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for !worker.allDone {
		worker.process()
	}
	worker.logPrintf("all finished!\n")

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
