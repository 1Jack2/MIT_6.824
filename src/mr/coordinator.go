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

type TaskState int

const (
	PENDING TaskState = iota
	RUNNING
	FINISHED
)

type Coordinator struct {
	// Your definitions here.
	m sync.Mutex

	files              []string
	mapState           []TaskState
	mapDispatchTime    []time.Time
	reduceState        []TaskState
	reduceDispatchTime []time.Time
	workerTimeOut      time.Duration
}

func (c *Coordinator) getTask() (no int, filename string, isReduce bool, allTaskFinished bool, err error) {
	c.m.Lock()
	defer c.m.Unlock()
	now := time.Now()
	allMapFinished := true
	for i, filename := range c.files {
		if c.mapState[i] == PENDING {
			c.mapState[i] = RUNNING
			c.mapDispatchTime[i] = now
			return i, filename, false, false, nil
		} else if c.mapState[i] == RUNNING && now.Sub(c.mapDispatchTime[i]) > c.workerTimeOut {
			c.mapDispatchTime[i] = now
			return i, filename, false, false, nil
		}
		if c.mapState[i] != FINISHED {
			allMapFinished = false
		}
	}

	// No PENDING map task, some map task is RUNNING
	if !allMapFinished {
		return -1, "", false, false, nil
	}

	for i := range c.reduceState {
		if c.reduceState[i] == PENDING {
			c.reduceState[i] = RUNNING
			c.reduceDispatchTime[i] = now
			return i, "", true, false, nil
		} else if c.reduceState[i] == RUNNING && now.Sub(c.reduceDispatchTime[i]) > c.workerTimeOut {
			c.reduceDispatchTime[i] = now
			return i, "", true, false, nil
		}
		if c.reduceState[i] != FINISHED {
			allMapFinished = false
		}
	}
	return -1, "", false, allTaskFinished, nil
}

func (c *Coordinator) finishTask(i int, isReduce bool) {
	c.m.Lock()
	defer c.m.Unlock()
	if isReduce {
		if c.reduceState[i] != RUNNING {
			log.Printf("reduce task-%d is not RUNNING\n", i)
		}
		log.Printf("reduce task-%d is FINISHED\n", i)
		c.reduceState[i] = FINISHED
	} else {
		if c.mapState[i] != RUNNING {
			log.Printf("map task-%d is not RUNNING\n", i)
		}
		log.Printf("map task-%d is FINISHED\n", i)
		c.mapState[i] = FINISHED
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	i, filename, isReduce, allTaskFinished, err := c.getTask()
	if err != nil {
		return err
	}

	reply.No = i
	reply.Filename = filename
	reply.NMap = len(c.mapState)
	reply.NReduce = len(c.reduceState)
	reply.IsReduce = isReduce
	reply.AllTaskFinished = allTaskFinished
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.finishTask(args.No, args.IsReduce)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.m.Lock()
	defer c.m.Unlock()
	for _, state := range c.mapState {
		if state != FINISHED {
			return false
		}
	}
	for _, state := range c.reduceState {
		if state != FINISHED {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.mapState = make([]TaskState, len(files))
	c.mapDispatchTime = make([]time.Time, len(files))
	now := time.Now()
	for i := 0; i < len(files); i++ {
		c.mapDispatchTime[i] = now
	}
	c.reduceState = make([]TaskState, nReduce)
	c.reduceDispatchTime = make([]time.Time, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceDispatchTime[i] = now
	}
	c.workerTimeOut = 10 * time.Second

	c.server()
	return &c
}
