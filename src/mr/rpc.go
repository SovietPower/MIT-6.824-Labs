package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here.

// request
type Request struct {
}

// task
type Task struct {
	TaskType   TaskType
	TaskID     int // 用于生成对应ID的文件
	NReduce    int // 用于取模
	InputFiles []string
}

// type of task
type TaskType int

const (
	MapTask = iota
	ReduceTask
	Wait // no available task for now
	Kill // all tasks done
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
