package main

import (

	"gopkg.in/couchbase/gocb.v1"
	//"../../../gopkg.in/couchbase/gocb.v1"
	"flag"
	//"runtime"
	"fmt"
	"os"
	"runtime"
	"bufio"
	"strconv"
	"time"
	"encoding/json"
)


type Document struct {
	Date  string `json:"date"`
	Text  string `json:"text"`
	Text2 string `json:"text2"`
}

type headerSlice []string

func (h *headerSlice) String() string {
	return fmt.Sprintf("%s", *h)
}

func (h *headerSlice) Set(value string) error {
	*h = append(*h, value)
	return nil
}
var (
	headerslice headerSlice
	server      = flag.String("s", "", "")
	bucket      = flag.String("b", "", "")
	dumpPath    = flag.String("d", "", "")
	password    = flag.String("w", "", "")
	partitions  = flag.Int("c", 0, "")
)

var usage = `Usage: gocbupload [options...] <url>

Options:
  -s  CB server name
  -b  CB bucket name
  -d  path folder with txt dump files
  -c  amount of partitions
  -w  CB password

`
type Counter struct {
	X int
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, fmt.Sprintf(usage, runtime.NumCPU()))
	}

	flag.Parse()
	if flag.NFlag() != 5 {
		flag.Usage()
		os.Exit(1)
	}

	c := &Counter{0}
	for i := 0; i < *partitions; i++ {
		go upload(i, c)
	}

	for c.X < *partitions {
		time.Sleep(time.Millisecond * 1000)
	}
}

func validateError(err error, hard bool) {
	if err != nil {
		fmt.Println("%v", err)
		if hard {
			os.Exit(1)
		}
	}
}

func upload(id int, c *Counter) {
	dumpSize := 100000
	filePath := *dumpPath + "/wiki_cb_dump_" + strconv.Itoa(id) + ".txt"
	fmt.Println("Uploading from file " + filePath)
	cluster, err := gocb.Connect("couchbase://" + *server)
	validateError(err, true)
	bucket, err := cluster.OpenBucket(*bucket, *password)
	validateError(err, true)
	file, err := os.Open(filePath)
	defer  file.Close()
	validateError(err, true)
	scanner := bufio.NewScanner(file)
	lines := 0

	for scanner.Scan() {
		keyInt := lines + (dumpSize * id)
		keyHex := fmt.Sprintf("%x", keyInt)
		var doc Document
		err = json.Unmarshal([]byte(scanner.Text()), &doc)
		_, err = bucket.Upsert(keyHex, doc, 0)
		validateError(err, false)
		lines ++
	}
	fmt.Println("Work is done for ", filePath, ". lines processed ", lines)
	c.X++
}