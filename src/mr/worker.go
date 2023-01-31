package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	wPid    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
)

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
		CallTaskDone(task)
	}

}

func process(task *Task) {
	switch task.TaskType {
	case EXIT:
		CallTaskDone(task)
		os.Exit(0)
	case MAP:
		processMap(task)
	case REDUCE:
		processReduce(task)
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
	// log.Println(intermediate)

	// write to mr-taskIdx-i{nReduce}
	nReduce := task.NReduce
	ofiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		// oname := "mr-" + idx + "-" + strconv.Itoa(i)
		// ofiles[i], _ = os.Create(oname)
		ofiles[i], _ = ioutil.TempFile("", wPid)
	}

	for _, kv := range intermediate {
		rIdx := ihash(kv.Key) % nReduce
		ofile := ofiles[rIdx]
		writeIntermediate(ofile, kv)
	}

	idx := strconv.Itoa(task.TaskIdx)
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + idx + "-" + strconv.Itoa(i)
		os.Rename(ofiles[i].Name(), oname)
	}

	for _, ofile := range ofiles {
		ofile.Close()
	}
}

func processReduce(task *Task) {
	// time.Sleep(time.Second)
	kva := []KeyValue{}
	nMap := task.NMap
	idx := strconv.Itoa(task.TaskIdx)
	for i := 0; i < nMap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + idx
		ifile, _ := os.Open(iname)
		kva = append(kva, readIntermediates(ifile)...)
		ifile.Close()
	}
	sort.Sort(ByKey(kva))

	ofile, _ := ioutil.TempFile("", wPid)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	oname := "mr-out-" + idx
	os.Rename(ofile.Name(), oname)

	ofile.Close()
	// log.Println(kva)
	// log.Println("Reduce completed")
}

// summary: workers use <call("master_func_name",args,reply)>
// where args and reply are interface{}, i.e. any type

func CallAskTask() MRReply {
	// log.Println("I'm worker", wPid, "I'm asking master for a task.")
	args := MRArgs{Message: "Worker" + wPid + "asking for task"}
	reply := MRReply{}
	// log.Println("about to call")
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		// log.Println("Worker received task", *reply.Task)
	} else {
		log.Fatalln("Call failed!")
	}
	return reply
}

func CallTaskDone(task *Task) {
	args := MRArgs{Message: "Worker " + wPid + " has done a task", Task: task}
	reply := MRReply{}

	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		// log.Println("Worker dealt with task", *args.Task)
	} else {
		log.Fatalln("Call failed!")
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
