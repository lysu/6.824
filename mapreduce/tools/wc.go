package main

import "os"
import "fmt"
import (
	"container/list"
	"strings"
	"unicode"
	"github.com/lysu/6824/mapreduce"
	"strconv"
)

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file content. the return
// value should be a list of key/value pairs, each represented
// by a mapreduce.KeyValue.
func MapLogic(value string) *list.List {
	words := strings.FieldsFunc(value, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	var result *list.List = list.New();
	for _, word := range words {
		result.PushBack(mapreduce.KeyValue{
			Key:   word,
			Value: "1",
		})
	}
	return result;
}

// called once for each key generated by Map, with a list
// of that key's string value. should return a single
// output value for that key.
func ReduceLogic(key string, values *list.List) string {
	count := 0;
	for e := values.Front(); e != nil; e = e.Next() {
		value, err := strconv.Atoi(e.Value.(string))
		if err != nil {
			panic(err)
		}
		count += value
	}
	return strconv.Itoa(count);
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], MapLogic, ReduceLogic)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], MapLogic, ReduceLogic, 100)
	}
}
