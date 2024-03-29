package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	No              int    // map task No
	Filename        string // map's input file name
	NMap            int    // map task count
	NReduce         int    // reduce task count
	IsReduce        bool   // true: reduce task, false: map task
	AllTaskFinished bool
}

type FinishTaskArgs struct {
	No       int
	IsReduce bool
}

type FinishTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
