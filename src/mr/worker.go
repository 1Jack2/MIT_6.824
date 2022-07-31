package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		log.Printf("got task isReduce=%v, no=%d, filename=%s, nMap=%d, nReduce=%d, allTaskFinished=%v\n",
			reply.IsReduce, reply.No, reply.Filename, reply.NMap, reply.NReduce, reply.AllTaskFinished)

		if !ok {
			log.Println("call failed")
			time.Sleep(time.Second)
		}
		if reply.AllTaskFinished {
			fmt.Println("all task finished, worker exit.")
			return
		}
		// no more PENDING task
		if reply.No < 0 {
			fmt.Println("no more PENDING task, sleep for a while")
			time.Sleep(time.Second)
			continue
		}
		// do task
		if reply.IsReduce {
			doReduceTask(reply.NMap, reply.No, reducef)
		} else {
			doMapTask(mapf, reply.Filename, reply.NReduce, reply.No)
		}
	}

}

func doReduceTask(nMap int, no int, reducef func(string, []string) string) {
	kva := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		iname := getMapOutputFileName(i, no)
		ifile, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %s", iname)
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", no)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j && k < len(kva); k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()

	err := notifyCoordinatorAfterFinishingTask(no, true)
	if err != nil {
		log.Println(err)
	}
}

func getMapOutputFileName(mapNo int, reduceNo int) string {
	return fmt.Sprintf("mr-%d-%d", mapNo, reduceNo)
}

func doMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, no int) {
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
	//log.Printf("kva size: %d", len(kva))
	buckets := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucketIdx := ihash(kv.Key) % nReduce
		//log.Printf("ihash(%s)->%d", kv.Key, bucketIdx)
		buckets[bucketIdx] = append(buckets[bucketIdx], kv)
	}

	for i := 0; i < nReduce; i++ {
		oFileName := fmt.Sprintf("mr-%d-%d", no, i)
		ofile, _ := os.Create(oFileName)
		enc := json.NewEncoder(ofile)
		//log.Printf("bucket[%d] size: %d", i, len(buckets[i]))
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		//ofile.Sync()
		ofile.Close()
	}

	for i := 0; i < nReduce; i++ {
		oldFileName := fmt.Sprintf("mr-%d-%d", no, i)
		newFileName := getMapOutputFileName(no, i)
		os.Rename(oldFileName, newFileName)
	}
	err = notifyCoordinatorAfterFinishingTask(no, false)
	if err != nil {
		log.Println(err)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func notifyCoordinatorAfterFinishingTask(no int, isReduce bool) error {
	args := FinishTaskArgs{no, isReduce}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		return errors.New("call failed")
	}
	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
