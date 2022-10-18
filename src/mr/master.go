package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"  // Temp Directory.
const TaskTimeout = 10 // Worker timeout.

// Define data type.
type TaskStatus int
type TaskType int
type JobStage int

const (
	MapTask    TaskType = iota // 0 iota 's function is count, like enum
	ReduceTask                 // 1
	NoTask                     // 2
	ExitTask                   // 3
)

const (
	NotStarted TaskStatus = iota // 0
	Executing                    // 1
	Finished                     // 2
)

// Define Task(map or reduce tasks) structure.
type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int
	File     string
	WorkerId int
}

// Define Master structure.
type Master struct {
	// Your definitions here.
	mu          sync.Mutex // Mutual exclusion locks
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// Args and Reply are defined in mr/rpc.go
//
// GetReduceCount RPC handler
//
func (m *Master) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	m.mu.Lock()         // Lock.
	defer m.mu.Unlock() // Unlock. defer is executed after return

	reply.ReduceCount = len(m.reduceTasks) // The number of Reduce tasks

	return nil // Predefined  represent zero value, only in pointer, channelm func etc.
}

//
// RequestTask RPC handler
//
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock() // Lock.

	var task *Task
	if m.nMap > 0 {
		task = m.selectTask(m.mapTasks, args.WorkerId)
	} else if m.nReduce > 0 {
		task = m.selectTask(m.reduceTasks, args.WorkerId)
	} else { // Tasks are finished.
		task = &Task{ExitTask, Finished, -1, "", -1}
	}

	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.TaskFile = task.File

	m.mu.Unlock()
	go m.waitForTask(task)

	return nil
}

//
// ReportTaskDone RPC handler
//
func (m *Master) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &m.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &m.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type: %v\n", args.TaskType)
		return nil
	}

	// Worker can only report task done except that the task was re-assigned due to timeout.
	if args.WorkerId == task.WorkerId && task.Status == Executing {
		task.Status = Finished
		if args.TaskType == MapTask && m.nMap > 0 {
			m.nMap-- // The number of Map task -1
		} else if args.TaskType == ReduceTask && m.nReduce > 0 {
			m.nReduce--
		}
	}

	reply.CanExit = m.nMap == 0 && m.nReduce == 0 // Exit with 1

	return nil
}

// Task selection.
func (m *Master) selectTask(taskList []Task, workerId int) *Task {
	var task *Task

	for i := 0; i < len(taskList); i++ {
		if taskList[i].Status == NotStarted {
			task = &taskList[i]
			task.Status = Executing
			task.WorkerId = workerId

			return task
		}
	}

	return &Task{NoTask, Finished, -1, "", -1}
}

// Wait for a task.
func (m *Master) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		// The task is not Map task or Reduce task.
		return
	}

	<-time.After(time.Second * TaskTimeout) // Timeout.

	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Status == Executing {
		// Task timeout, reset task status.
		task.Status = NotStarted
		task.WorkerId = -1
	}
}

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
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nReduce == 0 && m.nMap == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	nMap := len(files)
	m.nMap = nMap
	m.nReduce = nReduce
	m.mapTasks = make([]Task, 0, nReduce)

	for i := 0; i < nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		m.mapTasks = append(m.mapTasks, mTask)
	}
	for i := 0; i < nReduce; i++ {
		rTask := Task{ReduceTask, NotStarted, i, "", -1}
		m.reduceTasks = append(m.reduceTasks, rTask)
	}

	m.server()

	// Create temp directory
	outFiles, _ := filepath.Glob("mr-out*") // Glob: Pattern String.
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	err := os.RemoveAll(TempDir)
	if err != nil {
		log.Fatalf("Cannot remove temp directory %v\n", TempDir)
	}
	err = os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("Cannot create temp directory %v\n", TempDir)
	}

	return &m
}

