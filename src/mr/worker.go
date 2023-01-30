package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var (
	wPid    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// i.e. to which reduce task should key be sent?
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(_mapf func(string, string) []KeyValue,
	_reducef func(string, []string) string) {

	// Your worker implementation here.
	mapf = _mapf
	reducef = _reducef
	wPid = strconv.Itoa(os.Getpid())
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// callMaster()

	for {
		task := CallAskTask().Task
		process(task)
	}

}

func process(task *Task) {
	switch task.TaskType {
	case EXIT:
		log.Println("I will exit.")
		task.Stat = DONE
		os.Exit(0)
	case MAP:
		processMap(task)
		task.Stat = DONE
	case REDUCE:
		processReduce(task)
		task.Stat = DONE
	default:
		log.Fatalln("Failed to process task!", *task)
	}
}

func processMap(task *Task) {
	filename := task.Filename
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	log.Println(intermediate)

	// write to mr-taskIdx-i{nReduce}
	nReduce := task.NReduce
	idx := strconv.Itoa(task.TaskIdx)
	ofiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + idx + "-" + strconv.Itoa(i)
		ofiles[i], _ = os.Create(oname)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		key, val := intermediate[i].Key, intermediate[i].Value
		rIdx := ihash(key) % nReduce
		ofile := ofiles[rIdx]
		// this is the correct format for each line of Reduce output.
		for k := i; k < j; k++ {
			fmt.Printf("%v %v %d\n", key, val, rIdx)
			fmt.Fprintf(ofile, "%v %v\n", key, val)
		}
		i = j
	}

	log.Println("anyway, i'm mapping.")
	for i := 0; i < nReduce; i++ {
		ofiles[i].Close()
	}
}

func processReduce(task *Task) {
	log.Println("anyway, i'm reducing")
}

// summary: workers use <call("master_func_name",args,reply)>
// where args and reply are interface{}, i.e. any type

func CallAskTask() MRReply {
	log.Println("I'm worker", wPid, "I'm asking master for a task.")
	args := MRArgs{Message: "Worker" + wPid + "asking for task"}
	reply := MRReply{}

	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		log.Println("Worker received task", *reply.Task)
	} else {
		log.Fatalln("Call failed!")
	}
	return reply
}

func callMaster() {
	log.Println("Im worker. Ive sent a request to master for a task.")
	args := MRArgs{"Iwanta taks"}
	reply := MRReply{Message: "ee?"}

	ok := call("Coordinator.CallHandle", &args, &reply)
	if ok {
		fmt.Println("Worker Sent: ", args.Message, " and received: ", reply.Message)
	} else {
		fmt.Println("failed to call!")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()

	// log.Println("From worker, sockname is -" + sockname)

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
