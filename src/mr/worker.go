package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ProcessMapFunc(t *TaskResponse, mapf func(string, string) []KeyValue) {
	file, err := os.Open(t.MapFile)
	if err != nil {
		log.Fatalf("cannot open %v", t.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.MapFile)
	}
	file.Close()
	kvs := mapf(t.MapFile, string(content))
	nfiles := make([]*os.File, t.ReduceNum)
	for i := range nfiles {
		nfiles[i], _ = os.CreateTemp("./", fmt.Sprintf("mr-%d-%d.txt", t.MapId, i))
	}

	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.ReduceNum
		json.NewEncoder(nfiles[idx]).Encode(kv)
	}

	for i, f := range nfiles {
		f.Close()
		os.Rename(f.Name(), fmt.Sprintf("mr-%d-%d.txt", t.MapId, i))
	}

	nReq := NotifyDoneRequest{TaskType: TASK_MAP, TaskId: t.MapId}
	nResp := NotifyDoneResponse{}
	call("Coordinator.NotifiedTaskDone", &nReq, &nResp)
}

func ProcessReduceFunc(t *TaskResponse, reducef func(string, []string) string) {
	filenames, err := filepath.Glob("mr-*-" + strconv.Itoa(t.ReduceId) + ".txt")
	if err != nil {
		log.Fatalf("cannot find reduce fileId %v", t.ReduceId)
	}
	kvss := make([]*[]KeyValue, len(filenames)) //Q: copy or lock ?
	var wg sync.WaitGroup
	for i, filename := range filenames {
		wg.Add(1)
		kvss[i] = &[]KeyValue{}
		kvs := kvss[i]
		go func(f string) {
			defer wg.Done()
			file, err := os.Open(f)
			if err != nil {
				log.Fatalf("cannot open %v", t.MapFile)
			}
			defer file.Close()
			jdec := json.NewDecoder(file)
			var kv KeyValue
			for jdec.Decode(&kv) == nil {
				*kvs = append(*kvs, kv)
			}
		}(filename)
	}
	wg.Wait()
	ans := []KeyValue{}
	for _, kvs := range kvss {
		ans = append(ans, *kvs...)
	}
	sort.Sort(ByKey(ans))

	oname := "mr-out-" + strconv.Itoa(t.ReduceId) + ".txt"
	ofilet, err := os.CreateTemp("./", oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	i := 0
	for i < len(ans) {
		j := i + 1
		for j < len(ans) && ans[j].Key == ans[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, ans[k].Value)
		}
		output := reducef(ans[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofilet, "%v %v\n", ans[i].Key, output)

		i = j
	}
	ofilet.Close()
	os.Rename(ofilet.Name(), oname)

	nReq := NotifyDoneRequest{TaskType: TASK_REDUCE, TaskId: t.ReduceId}
	nResp := NotifyDoneResponse{}
	call("Coordinator.NotifiedTaskDone", &nReq, &nResp)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		req := TaskRequest{}
		resp := TaskResponse{}
		call("Coordinator.Coordinate", &req, &resp)
		switch resp.TaskType {
		case TASK_DONE:
			return
		case TASK_WAIT:
			time.Sleep(1 * time.Second)
			continue
		case TASK_MAP:
			ProcessMapFunc(&resp, mapf)
		case TASK_REDUCE:
			ProcessReduceFunc(&resp, reducef)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
