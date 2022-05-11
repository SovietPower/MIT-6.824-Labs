package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	TaskWaitPeriod = time.Second * 2
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	done := false

	for !done {
		// use RPC to the master and get the task
		task := CallDistributeTask()
		// fmt.Println("Get Task:", task)

		// worker implementation here
		switch task.TaskType {
		case MapTask:
			DoMapTask(mapf, task)
			CallTaskDone(task)
		case ReduceTask:
			DoReduceTask(reducef, task)
			CallTaskDone(task)
		case Wait:
			time.Sleep(TaskWaitPeriod)
		case Kill:
			done = true
			// fmt.Println("A worker finished.")
		default:
			fmt.Println("Worker(): Invalid TaskType:", task.TaskType)
		}

		// Request another task.

	}
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// Read each input file, pass it to Mapf and accumulate the intermediate Mapf output.
	filename := task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	// Divide the output into r parts and store them in mr-X-Y individually.
	r := task.NReduce
	prefix := "mr-" + strconv.Itoa(task.TaskID) + "-"

	// Divide them in advance to encode them by json.Encoder conveniently
	// The KVs with the same hash value of key need to be encoded and stored together.
	dividedKV := make([][]KeyValue, r)

	for _, kv := range intermediate {
		hs := ihash(kv.Key) % r
		dividedKV[hs] = append(dividedKV[hs], kv)
	}
	for i := 0; i < r; i++ {
		oname := prefix + strconv.Itoa(i)
		// ofile, _ := os.Create(oname) // Use a temporary file!

		dir, _ := os.Getwd()
		tempFile, err := ioutil.TempFile(dir, oname)
		if err != nil {
			log.Fatal("cannot create temp file", err)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range dividedKV[i] {
			enc.Encode(kv)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), oname)
	}
	// fmt.Println("finish DoMapTask")
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	// Read each input file and get the intermediate (all key/value pairs). Need to decode them from json.
	var tempKV KeyValue
	var intermediate []KeyValue
	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			if dec.Decode(&tempKV) != nil {
				break
			}
			intermediate = append(intermediate, tempKV)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(task.TaskID)
	// ofile, _ := os.Create(oname)

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, oname)
	if err != nil {
		log.Fatal("cannot create temp file", err)
	}

	// call Reduce on each distinct key in intermediate[],
	// and print the result to temp file mr-out-r.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), oname)

	// fmt.Println("finish DoReduceTask")
}

func CallDistributeTask() *Task {
	args := Request{}
	reply := Task{}

	call("Master.DistributeTask", &args, &reply)

	return &reply
}

func CallTaskDone(task *Task) {
	args := task
	var reply string
	call("Master.TaskDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
