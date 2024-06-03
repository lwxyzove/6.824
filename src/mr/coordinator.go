package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type task struct {
	sendt *time.Time
	done  bool
}

type mTask struct {
	task
	file string
}

type rdTask struct {
	task
}

const (
	MAP_PERIOD = iota
	REDUCE_PERIOD
	DONE
)

type Coordinator struct {
	// Your definitions here.

	mLock  sync.Mutex
	mTasks []*mTask

	rdTasks []*rdTask
	period  int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return c.period == DONE
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mTasks = make([]*mTask, 0, len(files))
	for _, f := range files {
		c.mTasks = append(c.mTasks, &mTask{file: f})
	}

	c.rdTasks = make([]*rdTask, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		c.rdTasks = append(c.rdTasks, new(rdTask))
	}
	c.period = MAP_PERIOD
	go c.watch()

	c.server()
	return &c
}

func (c *Coordinator) watch() {
	for {
		c.mLock.Lock()
		switch c.period {
		case MAP_PERIOD:
			for _, t := range c.mTasks {
				if t.done || t.sendt == nil {
					continue
				}
				if time.Since(*t.sendt) > 10*time.Second {
					t.sendt = nil
				}
			}
		case REDUCE_PERIOD:
			c.mLock.Lock()
			for _, t := range c.rdTasks {
				if t.done || t.sendt == nil {
					continue
				}
				if time.Since(*t.sendt) > 10*time.Second {
					t.sendt = nil
				}
			}
		case DONE:
			c.mLock.Unlock()
			return
		}
		c.mLock.Unlock()
		time.Sleep(2 * time.Second)
	}
}

func (c *Coordinator) Coordinate(req *TaskRequest, resp *TaskResponse) error {
	c.mLock.Lock()
	defer c.mLock.Unlock()
	now := time.Now()

	//fmt.Println("coordinate period: ", c.period)
	switch c.period {
	case DONE:
		resp.TaskType = TASK_DONE
	case MAP_PERIOD:
		for i, t := range c.mTasks {
			if t.done || t.sendt != nil {
				continue
			}
			t.sendt = &now
			resp.TaskType = TASK_MAP
			resp.MapId = i
			resp.MapFile = t.file
			resp.ReduceNum = len(c.rdTasks)
			break
		}
	case REDUCE_PERIOD:
		for i, t := range c.rdTasks {
			if t.done || t.sendt != nil {
				continue
			}
			t.sendt = &now
			resp.TaskType = TASK_REDUCE
			resp.ReduceId = i
			resp.ReduceNum = len(c.rdTasks)
			break
		}
	}
	if resp.TaskType == 0 {
		resp.TaskType = TASK_WAIT
	}
	return nil
}

func (c *Coordinator) NotifiedTaskDone(req *NotifyDoneRequest, resp *NotifyDoneResponse) error {
	c.mLock.Lock()
	defer c.mLock.Unlock()

	if req.TaskType == TASK_MAP {
		c.mTasks[req.TaskId].done = true
		for _, t := range c.mTasks {
			if !t.done {
				return nil
			}
		}
		c.period = REDUCE_PERIOD
	}

	if req.TaskType == TASK_REDUCE {
		c.rdTasks[req.TaskId].done = true
		for _, t := range c.rdTasks {
			if !t.done {
				return nil
			}
		}
		c.period = DONE
	}
	return nil
}
