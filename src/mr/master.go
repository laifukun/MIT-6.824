package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mapTask chan int
var reduceTask chan int

//var completedTaskID chan int

type Master struct {
	// Your definitions here.
	mu *sync.RWMutex

	totalReduceTask int
	totalMapTask    int

	nonAssignTaskID    int
	mapTaskFinished    bool
	reduceTaskFinished bool

	mapTaskIDToName  map[int]string
	mapTaskStatus    map[int]TaskStatus
	reduceTaskStatus map[int]TaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	//reply.Y = args.X + 1

	return nil
}

func (m *Master) MSGHandler(args *MRArgs, reply *MRReply) error {
	//fmt.Println(args.Message)
	switch args.Message {
	case REQTASK: //request for task

		select {
		case mapID := <-mapTask:

			reply.RTaskType = MAPTASK
			reply.Filename = m.mapTaskIDToName[mapID]
			reply.TotalReduceTask = m.totalReduceTask
			reply.TaskID = mapID

			m.mu.Lock()
			m.mapTaskStatus[mapID] = ALLOCATED
			m.mu.Unlock()
			fmt.Println("Map Task Assigned: ", mapID)

			go m.workerMonitor(MAPTASK, mapID)
			return nil

		case reduceID := <-reduceTask:

			reply.RTaskType = REDUCETASK
			reply.TaskID = reduceID
			reply.TotalMapTask = m.totalMapTask
			reply.TotalReduceTask = m.totalReduceTask

			m.mu.Lock()
			m.reduceTaskStatus[reduceID] = ALLOCATED
			m.mu.Unlock()
			fmt.Println("Reduce Task Assigned: ", reduceID)

			go m.workerMonitor(REDUCETASK, reduceID)
			return nil

		default:
			m.mu.RLock()
			allTaskFinished := m.mapTaskFinished && m.reduceTaskFinished
			m.mu.RUnlock()
			if allTaskFinished {
				reply.RTaskType = NOTASK
			} else {
				reply.RTaskType = WAIT
			}

			return nil

		}

	case SENDINFO: // Task Finished
		if args.STaskType == MAPTASK {

			m.mu.Lock()
			m.mapTaskStatus[args.TaskID] = COMPLETED
			m.mu.Unlock()

			return nil
		}

		if args.STaskType == REDUCETASK {

			m.mu.Lock()
			m.reduceTaskStatus[args.TaskID] = COMPLETED
			m.mu.Unlock()

			return nil
		}

	}

	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	go m.schedule()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	//err := http.ListenAndServe("unix", sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	fmt.Println("Start to Serve....")

}

func (m *Master) Done() bool {
	//m.mu.RLock()
	ret := false
	ret = m.reduceTaskFinished
	//m.mu.RUnlock()
	fmt.Println("All work completed ?", ret)
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	//m.mapTask <- 0
	m := Master{}
	// Your code here.
	m.initMaster(files, nReduce)
	m.server()
	return &m
}

func (m *Master) initMaster(files []string, nReduce int) {

	//fmt.Println(files)
	m.mapTaskIDToName = make(map[int]string)
	m.mapTaskStatus = make(map[int]TaskStatus)
	m.reduceTaskStatus = make(map[int]TaskStatus)
	m.mu = new(sync.RWMutex)
	mapTask = make(chan int, 5)
	reduceTask = make(chan int, 5)
	//completedTaskID = make(chan int, 1)
	m.totalReduceTask = nReduce
	m.totalMapTask = len(files)
	m.nonAssignTaskID = 0
	for i, filename := range files {
		m.mapTaskIDToName[i] = filename
		m.mapTaskStatus[i] = UNALLOCATED
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskStatus[i] = UNALLOCATED
	}

	m.reduceTaskFinished = false
}

func (m *Master) checkTaskStatus(taskType TaskType) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	switch taskType {
	case MAPTASK:
		for _, st := range m.mapTaskStatus {
			if st != COMPLETED {
				return false
			}
		}
	case REDUCETASK:
		for _, st := range m.reduceTaskStatus {
			if st != COMPLETED {
				return false
			}
		}
	}

	return true
}

func (m *Master) schedule() {
	//m.mu.RLock()
	//defer m.mu.RUnlock()
	/*
		for i, st := range m.mapTaskStatus {

			if st == UNALLOCATED {
				mapTask <- i
			}
		}
	*/

	for m.nonAssignTaskID < m.totalMapTask {

		mapTask <- m.nonAssignTaskID
		m.mu.Lock()
		m.nonAssignTaskID += 1
		m.mu.Unlock()

	}
	mapCompleted := false

	for !mapCompleted {
		mapCompleted = m.checkTaskStatus(MAPTASK)
		//fmt.Println("All Map Completed ?: ", mapCompleted)
	}

	m.mu.Lock()
	m.mapTaskFinished = true
	m.nonAssignTaskID = 0
	m.mu.Unlock()
	fmt.Println("All Map Task Completed!")
	/*	for i, st := range m.reduceTaskStatus {
			if st == UNALLOCATED {
				reduceTask <- i
			}
		}
	*/
	for m.nonAssignTaskID < m.totalReduceTask {

		reduceTask <- m.nonAssignTaskID
		m.mu.Lock()
		m.nonAssignTaskID += 1
		m.mu.Unlock()

	}
	reduceCompleted := false

	for !reduceCompleted {
		reduceCompleted = m.checkTaskStatus(REDUCETASK)
	}
	m.mu.Lock()
	m.reduceTaskFinished = true
	m.mu.Unlock()
	fmt.Println("All Reduce Task Completed!")
}

func (m *Master) workerMonitor(taskType TaskType, taskID int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Delayed response: ", taskType, taskID)
			if taskType == MAPTASK {
				m.mu.Lock()
				m.mapTaskStatus[taskID] = UNALLOCATED
				m.mu.Unlock()
				mapTask <- taskID

				return
			}
			if taskType == REDUCETASK {
				m.mu.Lock()
				m.reduceTaskStatus[taskID] = UNALLOCATED
				m.mu.Unlock()
				reduceTask <- taskID
				return
			}
		default:

			switch taskType {

			case MAPTASK:
				m.mu.RLock()
				taskCompleted := (m.mapTaskStatus[taskID] == COMPLETED)
				m.mu.RUnlock()

				if taskCompleted {
					fmt.Println("Task Completed: ", taskType, taskID)
					return
				}

			case REDUCETASK:

				m.mu.RLock()
				taskCompleted := (m.reduceTaskStatus[taskID] == COMPLETED)
				m.mu.RUnlock()

				if taskCompleted {
					fmt.Println("Task Completed: ", taskType, taskID)
					return
				}

			}

			//case compl := <-completedTaskID:
			//	fmt.Println("Completed Task ID: ", compl)
			//	return
		}
	}
}
