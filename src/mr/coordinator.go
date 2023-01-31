package mr

import (
	"fmt"
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
	Id            int    // task identifier
	TaskIdx       int    // start from 0. note: mr-x-y file needs it
	Filename      string // Map task needs it to read input
	NMap, NReduce int    // to write or read inter-kv
	TaskType      int    // map/reduce/exit
	Stat          int    // not alloc, allocated, done
	CreateTime    time.Time
}

// Your code here -- RPC handlers for the worker to call.

var (
	nReduce                int
	nMap                   int
	mapIdx                 int
	reduceIdx              int
	cPid                   string
	taskNum                int
	idLock, miLock, riLock sync.Mutex
	runLock                sync.Mutex
	finLock                sync.Mutex
	mapDone                sync.WaitGroup
	reduceDone             sync.WaitGroup
)

var (
	todoQueue    chan *Task
	runningQueue chan *Task
	doneQueue    chan *Task
	runningSet   sync.Map
	exiting      chan int
)

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskTask(args *MRArgs, reply *MRReply) error {
	// log.Println("Master", cPid, "received", *args)
	// if rFinished {
	// 	todoQueue <- c.newTask("", EXIT)
	// }
	// reply.Task = <-todoQueue
	// finLock.Lock()
	// if rFinished {
	// 	reply.Task = c.newTask("", EXIT)
	// } else {
	// 	reply.Task = <-todoQueue
	// }
	// finLock.Unlock()
	t := <-todoQueue
	// runningQueue <- t
	runningSet.Store(t.Id, t)
	go monitor(t)

	reply.Task = t
	reply.Task.Stat = RUNNING
	// log.Println("Master sent task", *reply.Task)
	reply.Message = "Master" + cPid + "sent a task."

	return nil
}

func (c *Coordinator) TaskDone(args *MRArgs, reply *MRReply) error {
	// log.Println("Master", cPid, "received", *args.Task)
	task := args.Task
	runLock.Lock()
	// t := <-runningQueue
	// for t.Id != task.Id {
	// 	runningQueue <- t
	// 	t = <-runningQueue
	// }
	iface, ok := runningSet.Load(task.Id)
	if !ok {
		runLock.Unlock()
		return nil
	}
	t := iface.(*Task)
	t.Stat = DONE
	runningSet.Delete(t.Id)
	runLock.Unlock()
	// doneQueue <- t
	switch t.TaskType {
	case MAP:
		// mdLock.Lock()
		// nMapDone++
		// if nMapDone == nMap {
		// 	nMapDone = 0
		// 	go c.generateReduceTasks()
		// }
		// mdLock.Unlock()
		mapDone.Done()
	case REDUCE:
		// rdLock.Lock()
		// nReduceDone++
		// finLock.Lock()
		// if !rFinished && nReduceDone == nReduce {
		// 	todoQueue <- c.newTask("", EXIT)
		// 	rFinished = true
		// }
		// finLock.Unlock()
		// rdLock.Unlock()
		reduceDone.Done()
	}
	// log.Println(*args.Task, "done")
	return nil
}

func (c *Coordinator) generateMapTasks(filenames []string) {
	for _, filename := range filenames {
		task := c.newTask(filename, MAP)
		todoQueue <- task
	}
}
func (c *Coordinator) generateReduceTasks() {
	mapDone.Wait()
	for i := 0; i < nReduce; i++ {
		task := c.newTask("", REDUCE)
		// log.Println("a")
		todoQueue <- task
		// log.Println("b")
	}
}

func (c *Coordinator) generateExitTasks() {
	reduceDone.Wait()
	for {
		go func() {
			exiting <- 1
		}()
		_, ok := <-exiting
		if !ok {
			return
		}
		task := c.newTask("", EXIT)
		todoQueue <- task
	}
}

func (c *Coordinator) newTask(filename string, taskType int) *Task {
	task := &Task{
		Filename: filename,
		TaskType: taskType,
		NReduce:  nReduce,
		NMap:     nMap,
		Stat:     TODO,
	}

	idLock.Lock()
	task.Id = taskNum
	taskNum++
	idLock.Unlock()

	switch taskType {
	case MAP:
		miLock.Lock()
		task.TaskIdx = mapIdx
		mapIdx++
		miLock.Unlock()
	case REDUCE:
		riLock.Lock()
		task.TaskIdx = reduceIdx
		reduceIdx++
		riLock.Unlock()
	default:
		task.TaskIdx = 0
	}
	return task
}

func monitor(t *Task) {
	time.Sleep(10 * time.Second)
	runLock.Lock()
	if t.Stat == RUNNING {
		log.Println(*t, "has run for 10s")
		runningSet.Delete(t.Id)
		t.Stat = TODO
		todoQueue <- t
	}
	runLock.Unlock()

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
	// log.Println("Enter server")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	reduceDone.Wait()

	time.Sleep(time.Second)

	lenTodo := len(todoQueue)
	if lenTodo == 0 {
		return false
	}
	lenRun := 0
	runningSet.Range(func(k, v interface{}) bool {
		lenRun++
		return true
	})
	return lenRun == 0

	// done := len(todoQueue) != 0 && len(runningSet) == 0
	// return done
	// Your code here.
	// finLock.Lock()
	// defer finLock.Unlock()
	// return rFinished && len(todoQueue) == 0 && len(runningQueue) == 0
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
	cPid = strconv.Itoa(os.Getpid())
	nReduce, nMap = _nReduce, len(files)
	todoQueue = make(chan *Task, nMap)
	runningQueue = make(chan *Task, nMap)
	doneQueue = make(chan *Task, nMap)

	exiting = make(chan int)
	mapDone.Add(nMap)
	reduceDone.Add(nReduce)

	go c.generateMapTasks(files)
	go c.generateReduceTasks()
	go c.generateExitTasks()
	// go c.LogQueue(todoQueue)
	c.server()

	return &c
}
