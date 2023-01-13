package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int
	TaskId            int
	DistPhase         Phase
	TaskChannelMap    chan *Task
	TaskChannelReduce chan *Task
	taskMetaHolder    TaskMetaHolder
	files             []string
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

type TaskMetaInfo struct {
	state   State
	TaskAdr *Task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			Filename:   v,
		}

		taskMetaInfo := TaskMetaInfo{
			state:   Waiting,
			TaskAdr: &task,
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("Make a map task: ", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
				} else {
					reply.TaskType = WaitingTask
					if c.taskMetaHolder.checkTaskDone() {
						c.toNextPhase()
					}
					return nil
				}
			}
		}
	default:
		{
			reply.TaskType = ExitTask
		}
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase || c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum       = 0
		mapProcessNum    = 0
		reduceDoneNum    = 0
		reduceProcessNum = 0
	)

	for _, v := range t.MetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapProcessNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceProcessNum++
			}
		}
	}

	if (mapDoneNum > 0 && mapProcessNum == 0) && (reduceDoneNum == 0 && reduceProcessNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceProcessNum == 0 {
			return true
		}
	}
	return false
}

func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	return true
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]
		if ok && meta.state == Working {
			meta.state = Done
			fmt.Printf("Map task Id[%d] is finished. \n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] has already finished.\n", args.TaskId)
		}
		break
	default:
		panic("Undefined task")
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished, the coordinator will be exit.\n")
		return true
	}
	return false

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        numReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, numReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+numReduce),
		},
	}
	c.makeMapTasks(files)

	c.server()
	return &c
}
