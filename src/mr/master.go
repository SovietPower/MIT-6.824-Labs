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

const (
	ExpireTime = time.Second * 10
	WaitPeriod = time.Second * 3
)

var mutex sync.Mutex    // a mutex for m.Status
var rc_mutex sync.Mutex // a mutex for read_count
var read_count int      // a read_count for m.Status

var taskStatus_mutex sync.Mutex // a mutex for TaskStatus

// Master
type Master struct {
	MapTaskChannel    chan *Task
	ReduceTaskChannel chan *Task

	NMap            int // number of Map tasks
	NReduce         int // number of Reduce tasks
	NTask           int // number of current tasks
	NUnfinishedTask int // number of unfinished tasks
	Status          MasterStatus

	TaskInfoMap map[int]*TaskInfo
}

// status of master
type MasterStatus int

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

// info of task
type TaskInfo struct {
	TaskStatus TaskStatus
	ExpiredAt  time.Time
	Task       *Task
}

// status of task
type TaskStatus int

const (
	TaskWaiting = iota
	TaskRunning
	TaskFinished
)

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),

		NMap:    len(files),
		NReduce: nReduce,
		Status:  MapPhase,
	}

	m.MakeMapTask(files)

	m.server()
	go m.TimeoutHandler()

	return &m
}

// The start of Map phase. Generate Map Tasks and store them in MapTaskChannel.
func (m *Master) MakeMapTask(files []string) {
	m.TaskInfoMap = make(map[int]*TaskInfo, len(files))

	for _, v := range files {
		id := m.GetTaskID()

		task := Task{
			TaskType:   MapTask,
			TaskID:     id,
			NReduce:    m.NReduce,
			InputFiles: []string{v},
		}
		taskInfo := TaskInfo{
			TaskStatus: TaskWaiting,
			Task:       &task,
		}
		m.NewTaskInfo(&taskInfo)

		m.MapTaskChannel <- &task
		// fmt.Println("Generate Map Task:", task)
	}

	m.NUnfinishedTask = m.NTask
	// fmt.Println("finish MakeMapTask")
}

// The start of Reduce phase. Generate Reduce Tasks and store them in ReduceTaskChannel.
func (m *Master) MakeReduceTask() {
	m.TaskInfoMap = make(map[int]*TaskInfo, m.NReduce)

	m.NTask = 0 // redistribute all the id
	for i := 0; i < m.NReduce; i++ {
		id := m.GetTaskID()

		task := Task{
			TaskType:   ReduceTask,
			TaskID:     id,
			InputFiles: m.GetReduceInputFiles(i),
		}
		taskInfo := TaskInfo{
			TaskStatus: TaskWaiting,
			Task:       &task,
		}
		m.NewTaskInfo(&taskInfo)

		m.ReduceTaskChannel <- &task
		// fmt.Println("Generate Reduce Task:", task)
	}

	m.NUnfinishedTask = m.NTask
	// fmt.Println("finish MakeReduceTask")
}

// Generate the file names that the reduce worker needs (mr-*-y).
func (m *Master) GetReduceInputFiles(rid int) []string {
	var s []string
	suffix := "-" + strconv.Itoa(rid)
	for i := 0; i < m.NMap; i++ {
		s = append(s, "mr-"+strconv.Itoa(i)+suffix) // mr-*-rid
		// todo: need to validate the validity of the file
	}
	return s
}

// Get an ID for a new task.
func (m *Master) GetTaskID() int {
	m.NTask++
	return m.NTask - 1
}

// Store a taskInfo in Master.TaskInfoMap.
func (m *Master) NewTaskInfo(taskInfo *TaskInfo) {
	id := taskInfo.Task.TaskID
	value, _ := m.TaskInfoMap[id]
	if value != nil {
		fmt.Println("TaskInfo conflicted:", id, value, taskInfo)
	} else {
		m.TaskInfoMap[id] = taskInfo
	}
}

// Get the taskInfo by taskID.
func (m *Master) GetTaskInfo(taskID int) (*TaskInfo, bool) {
	taskInfo, ok := m.TaskInfoMap[taskID]
	if !ok {
		fmt.Println("No TaskInfo with TaskID:", taskID)
		return taskInfo, false
	}
	return taskInfo, true
}

// Update TaskStatus and ExpiredAt.
// Be aware that TaskStatus may be Waiting, Running or Finished.
// When TaskStatus is Finished, the function can just return false(with ExpiredAt not reset) or change TaskType to Wait.
func (m *Master) UpdateTaskInfo(taskID int) bool {
	taskStatus_mutex.Lock()
	defer taskStatus_mutex.Unlock()

	taskInfo, ok := m.GetTaskInfo(taskID)
	if !ok || taskInfo.TaskStatus != TaskWaiting {
		return false
	}
	taskInfo.ExpiredAt = time.Now().Add(ExpireTime)
	taskInfo.TaskStatus = TaskRunning
	return true
}

// RPC handlers for the worker to call.
// When Worker is started or finishes its task, it calls DistributeTask() to get a new task.
// When there is no task available, Master sends a Wait task to tell Worker to wait for a few seconds before calling again.
// When everything is finished, Master sends a Kill task and the Worker then finish.
func (m *Master) DistributeTask(args *Request, reply *Task) error {
	ReaderLock()
	defer ReaderUnlock()

	// fmt.Println("begin DistributeTask")

	switch m.Status {
	case MapPhase:
		if len(m.MapTaskChannel) > 0 {
			*reply = *<-m.MapTaskChannel
			// reply = <-m.MapTaskChannel // 错。reply为传值引用，修改reply不会影响worker处的reply
			if !m.UpdateTaskInfo(reply.TaskID) {
				fmt.Println("No such TaskInfo or Task", reply.TaskID, "runs again.")
			}
		} else {
			reply.TaskType = Wait
		}
	case ReducePhase:
		if len(m.ReduceTaskChannel) > 0 {
			*reply = *<-m.ReduceTaskChannel
			if !m.UpdateTaskInfo(reply.TaskID) {
				fmt.Println("No such TaskInfo or Task", reply.TaskID, "runs again.")
			}
		} else {
			reply.TaskType = Wait
		}
	case AllDone:
		reply.TaskType = Kill
	}

	return nil
}

// A task is finished.
func (m *Master) TaskDone(task *Task, reply *string) error {
	taskStatus_mutex.Lock()
	defer taskStatus_mutex.Unlock()

	info, ok := m.GetTaskInfo(task.TaskID)
	if !ok {
		fmt.Println("Invalid TaskID:", task.TaskID)
	} else if info.TaskStatus != TaskFinished {
		// Be aware that the TaskStatus of an undone task may be TaskWaiting or TaskRunning.
		// fmt.Println("Task", task.TaskID, "finished.")

		m.NUnfinishedTask--
		info.TaskStatus = TaskFinished

		if m.CurrentPhaseDone() {
			m.NextPhase()
		}
	} else {
		// fmt.Println("Task", task.TaskID, "got a timeout and finished again.")
	}
	return nil
}

// Whether all the tasks are finished or not.
func (m *Master) CurrentPhaseDone() bool {
	return m.NUnfinishedTask == 0
}

// Change status from MapPhase to ReducePhase, or from ReducePhase to All Done.
func (m *Master) NextPhase() {
	mutex.Lock()
	defer mutex.Unlock()
	// fmt.Println("NextPhase!", m.Status)

	switch m.Status {
	case MapPhase:
		m.Status = ReducePhase
		m.MakeReduceTask()
	case ReducePhase:
		m.Status = AllDone
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	ReaderLock()
	defer ReaderUnlock()
	return m.Status == AllDone
}

// main/mrmaster.go calls TimeoutHandler() periodically to detect time-outs and redistribute these tasks.
func (m *Master) TimeoutHandler() {
	for {
		time.Sleep(WaitPeriod)

		ReaderLock() // 注意不是defer Unlock()，是在每次循环后
		taskStatus_mutex.Lock()
		if m.Status == AllDone {
			ReaderUnlock()
			taskStatus_mutex.Unlock()
			return
		}

		now := time.Now()
		for _, v := range m.TaskInfoMap {
			// Don't redistribute finished tasks.
			if v.TaskStatus == TaskRunning && v.ExpiredAt.Before(now) {
				// fmt.Println("Timeout detected:", v.Task.TaskID, v.Task.TaskType, v.ExpiredAt, now)

				v.TaskStatus = TaskWaiting
				switch v.Task.TaskType {
				case MapTask:
					m.MapTaskChannel <- v.Task
					break
				case ReduceTask:
					m.ReduceTaskChannel <- v.Task
					break
				}
			}
		}

		ReaderUnlock()
		taskStatus_mutex.Unlock()
	}
}

// Lock
func ReaderLock() {
	rc_mutex.Lock()
	if read_count++; read_count == 1 {
		mutex.Lock()
	}
	rc_mutex.Unlock()
}

// Unlock
func ReaderUnlock() {
	rc_mutex.Lock()
	if read_count--; read_count == 0 {
		mutex.Unlock()
	}
	rc_mutex.Unlock()
}
