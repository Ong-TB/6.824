package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

// Task type
const (
	MAP    = 0
	REDUCE = 1
	WAIT   = 3
	EXIT   = 4
)

// Task status
const (
	TODO    = 0
	RUNNING = 1
	DONE    = 2
)

type Coordinator struct {
	// Your definitions here.

}

type Task struct {
	TaskIdx       int    // start from 0. note: mr-x-y file needs it
	Filename      string // Map task needs it to read input
	NMap, NReduce int    // to write or read inter-kv
	TaskType      int    // map/reduce/wait/exit
	Stat          int    // not alloc, allocated, done
}

// Your code here -- RPC handlers for the worker to call.

var (
	nReduce   int
	nMap      int
	mapIdx    int
	reduceIdx int
	mPid      string
)

var (
	todoQueue    chan *Task
	runningQueue chan *Task
	doneQueue    chan *Task
)

func (c *Coordinator) CallHandle(args *MRArgs, reply *MRReply) error {
	// reply = "Haha I'm not functioning yet."
	reply.Message = "Err? But im not functioning now."
	log.Println("Master received: ", args.Message, " and sent: ", reply.Message)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *MRArgs, reply *MRReply) error {
	log.Println("Master", mPid, "received", *args)
	reply.Task = <-todoQueue
	reply.Message = "Master" + mPid + "sent a task."
	return nil
}

func (c *Coordinator) generateMapTasks(filenames []string) {
	for _, filename := range filenames {
		task := c.newTask(filename, MAP)
		todoQueue <- task
	}
	todoQueue <- c.newTask("ee", EXIT)
}

func (c *Coordinator) newTask(filename string, taskType int) *Task {
	task := &Task{
		Filename: filename,
		TaskType: taskType,
		NReduce:  nReduce,
		NMap:     nMap,
		Stat:     TODO,
	}
	switch taskType {
	case MAP:
		task.TaskIdx = mapIdx
		mapIdx++
	case REDUCE:
		task.TaskIdx = reduceIdx
		reduceIdx++
	default:
		task.TaskIdx = 0
	}
	return task
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Enter server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) LogQueue(ch chan *Task) {
	for task := range ch {
		fmt.Println(*task)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, _nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	mPid = strconv.Itoa(os.Getpid())
	nReduce = 3
	nMap = len(files)
	todoQueue = make(chan *Task, nMap)
	runningQueue = make(chan *Task, nMap)
	doneQueue = make(chan *Task, nMap)

	go c.generateMapTasks(files)

	// go c.LogQueue(todoQueue)
	c.server()

	return &c
}
