package mr

import (
	"encoding/json"
	"hash/fnv"
	"os"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// i.e. to which reduce task should key be sent?
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeIntermediate(file *os.File, intermediate KeyValue) error {
	enc := json.NewEncoder(file)
	// for _, i := range intermediate {
	// 	err := enc.Encode(&i)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	err := enc.Encode(&intermediate)
	return err
}

func readIntermediates(file *os.File) []KeyValue {
	dec := json.NewDecoder(file)
	var kva []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break

		}
		kva = append(kva, kv)
	}
	return kva
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
