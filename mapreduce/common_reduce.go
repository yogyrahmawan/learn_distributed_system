package mapreduce

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	log.Printf("start reduce job name = %s, reducetask = %d \n", jobName, reduceTask)

	result := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		f, err := os.Open(fileName)
		if err != nil {
			log.Printf("error open file, err = %s", err)
			return
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var kv KeyValue
			val := scanner.Text()
			err := json.Unmarshal([]byte(val), &kv)
			if err != nil {
				log.Printf("error unmarshal, value = %s, err = %s", val, err)
				f.Close()
				return
			}
			result[kv.Key] = append(result[kv.Key], kv.Value)
		}
		f.Close()
	}

	// sort
	var keys []string
	for k := range result {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// create file
	of, err := os.Create(outFile)
	if err != nil {
		log.Printf("error create out file , err = %s", err)
		return
	}
	defer of.Close()
	// merge
	enc := json.NewEncoder(of)
	for _, v := range keys {
		mapVal := result[v]
		if err := enc.Encode(KeyValue{v, reduceF(v, mapVal)}); err != nil {
			log.Printf("error encode value, err = %s", err)
			return
		}
	}

	log.Printf("end reduce job name = %s, reduceTask = %d \n", jobName, reduceTask)
}
