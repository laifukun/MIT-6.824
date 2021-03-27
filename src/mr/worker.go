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

	//	CallExample(mapf, reducef)
	// Your worker implementation here.

	for true {
		reply := requestTask()
		//fmt.Println("reply.TaskType: ", reply.RTaskType)
		switch reply.RTaskType {

		case MAPTASK:
			mapWork(&reply, mapf)
			finishTask(MAPTASK, reply.TaskID)
		case REDUCETASK:
			reduceWork(&reply, reducef)
			finishTask(REDUCETASK, reply.TaskID)
		case WAIT:
			time.Sleep(1 * time.Second)
		case NOTASK:
			return
		}
	}

	//fmt.Printf("reply.filename %v\n", reply.Filename)
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func mapWork(reply *MRReply, mapf func(string, string) []KeyValue) {
	filename := reply.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	kvBuck := partitionToBucket(reply.TotalReduceTask, kva)

	for i := 0; i < reply.TotalReduceTask; i++ {
		intname := "mr-" + strconv.Itoa(reply.TaskID) + "-" + strconv.Itoa(i)
		intfile, _ := os.Create(intname)
		enc := json.NewEncoder(intfile)

		for _, kv := range kvBuck[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
		intfile.Close()
		//os.Rename(intfile.Name(), intname)

	}

	//fmt.Println("Completed Map Task ", reply.TaskID)

}

func reduceWork(reply *MRReply, reducef func(string, []string) string) {

	intermediate := []KeyValue{}
	//interFilename := []string{}
	for i := 0; i < reply.TotalMapTask; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskID)
		//fmt.Println(filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
		//interFilename = append(interFilename, filename)
	}
	//fmt.Println("Intermediate Length  ", len(intermediate))
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, _ := os.Create(oname)

	//fmt.Printf("Reduced work: %v, %v", oname, len(intermediate))
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("write error %v", oname)
		}
		i = j
	}
	//fmt.Println("Completed Reduce Task ", reply.TaskID)
	ofile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	//intermediate := []KeyValue{}

	filename := reply.Y
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	enc := json.NewEncoder(ofile)

	for _, kv := range kva {
		enc.Encode(&kv)
	}

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func requestTask() MRReply {
	// declare an argument structure.
	args := MRArgs{}

	// fill in the argument(s).
	args.Message = REQTASK

	// declare a reply structure.
	reply := MRReply{}

	// send the RPC request, wait for the reply.
	call("Master.MSGHandler", &args, &reply)

	//fmt.Println(reply.Filename)
	return reply
}

func finishTask(taskType TaskType, taskID int) {
	args := MRArgs{}

	args.Message = SENDINFO
	args.STaskType = taskType
	args.TaskID = taskID
	args.STaskStatus = COMPLETED

	reply := MRReply{}
	call("Master.MSGHandler", &args, &reply)

	//fmt.Println(reply.Filename)
	//	return reply
}

/*
func finishReduceTask(taskType string, reduceNumber int) {
	args := MRArgs{}

	args.Message = 1
	args.FinishedTask = taskType
	args.ReduceTaskNumber = reduceNumber

	reply := MRReply{}
	call("Master.MSGHandler", &args, &reply)

	//fmt.Println(reply.Filename)
	//	return reply
}
*/

func partitionToBucket(n int, kvlist []KeyValue) [][]KeyValue {
	kvBucket := make([][]KeyValue, n)

	for _, kv := range kvlist {
		v := ihash(kv.Key) % n
		kvBucket[v] = append(kvBucket[v], kv)
	}
	return kvBucket
}

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
	fmt.Println("call failed")
	return false
}
