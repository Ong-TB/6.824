package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Task type
const (
	MAP    = 0
	REDUCE = 1
	EXIT   = 2
)

// Task status
const (
	TODO    = 0
	RUNNING = 1
	DONE    = 2
)

type Coordinator struct {
	// Your definitions here.
	cPid                   string
	nReduce                int
	nMap                   int
	taskNum                int
	mapIdx                 int
	reduceIdx              int
	idLock, miLock, riLock sync.Mutex
	mapDone                sync.WaitGroup
	reduceDone             sync.WaitGroup

	todoQueue  chan *Task
	runningSet sync.Map
}

type Task struct {
	Id            int    // task identifier
	TaskIdx       int    // start from 0. note: mr-x-y file needs it
	Filename      string // Map task needs it to read input
	NMap, NReduce int    // to write or read inter-kv
	TaskType      int    // map/reduce/exit
	stat          int    // todo, running, done
	tlock         sync.Mutex
}

func (t *Task) SetStat(stat int) {
	t.tlock.Lock()
	t.stat = stat
	t.tlock.Unlock()
}

func (t *Task) GetStat() int {
	t.tlock.Lock()
	stat := t.stat
	t.tlock.Unlock()
	return stat
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *MRArgs, reply *MRReply) error {
	t := <-c.todoQueue
	t.SetStat(RUNNING)
	c.runningSet.Store(t.Id, t)

	go c.monitor(t)

	reply.Task = t
	reply.Message = "Master" + c.cPid + "sent a task."

	return nil
}

func (c *Coordinator) TaskDone(args *MRArgs, reply *MRReply) error {
	task := args.Task
	iface, ok := c.runningSet.Load(task.Id)
	if !ok {
		// if task is not in running set,
		// it's an invalid completion
		return nil
	}

	t := iface.(*Task)
	t.SetStat(DONE)
	c.runningSet.Delete(t.Id)

	switch t.TaskType {
	case MAP:
		c.mapDone.Done()
	case REDUCE:
		c.reduceDone.Done()
	}
	return nil
}

func (c *Coordinator) generateMapTasks(filenames []string) {
	for _, filename := range filenames {
		task := c.newTask(filename, MAP)
		c.todoQueue <- task
	}
}
func (c *Coordinator) generateReduceTasks() {
	c.mapDone.Wait()
	for i := 0; i < c.nReduce; i++ {
		task := c.newTask("", REDUCE)
		c.todoQueue <- task
	}
}

func (c *Coordinator) generateExitTasks() {
	c.reduceDone.Wait()
	for {
		task := c.newTask("", EXIT)
		c.todoQueue <- task
	}
}

func (c *Coordinator) newTask(filename string, taskType int) *Task {
	task := &Task{
		Filename: filename,
		TaskType: taskType,
		NReduce:  c.nReduce,
		NMap:     c.nMap,
		stat:     TODO,
	}

	c.idLock.Lock()
	task.Id = c.taskNum
	c.taskNum++
	c.idLock.Unlock()

	switch taskType {
	case MAP:
		c.miLock.Lock()
		task.TaskIdx = c.mapIdx
		c.mapIdx++
		c.miLock.Unlock()
	case REDUCE:
		c.riLock.Lock()
		task.TaskIdx = c.reduceIdx
		c.reduceIdx++
		c.riLock.Unlock()
	default:
		task.TaskIdx = 0
	}
	return task
}

func (c *Coordinator) monitor(t *Task) {
	time.Sleep(10 * time.Second)

	if t.GetStat() == RUNNING {
		c.runningSet.Delete(t.Id)
		t.SetStat(TODO)
		c.todoQueue <- t
	}
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.reduceDone.Wait()

	time.Sleep(time.Second)

	lenTodo := len(c.todoQueue)
	if lenTodo == 0 {
		return false
	}

	lenRun := 0
	c.runningSet.Range(func(k, v interface{}) bool {
		lenRun++
		return true
	})
	return lenRun == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, _nReduce int) *Coordinator {
	c := Coordinator{
		cPid:      strconv.Itoa(os.Getpid()),
		nReduce:   _nReduce,
		nMap:      len(files),
		todoQueue: make(chan *Task, len(files)),
	}

	// Your code here.

	c.mapDone.Add(c.nMap)
	c.reduceDone.Add(c.nReduce)

	go c.generateMapTasks(files)
	go c.generateReduceTasks()
	go c.generateExitTasks()

	c.server()

	return &c
}
