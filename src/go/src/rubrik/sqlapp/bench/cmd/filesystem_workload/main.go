package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"rubrik/sqlapp/bench/filesys"
	"rubrik/util/randutil"
)

func main() {
	layout := flag.Bool(
		"layout",
		false,
		`Lay out metadata for workload. This seeds the database with files
		and stripes that can be used for reads on a subsequent invocation 
		of the same workload.`,
	)
	drop := flag.Bool("drop", false, "Drop tables before starting.")
	numWriters := flag.Int("num_writers", 0, "Number of writers.")
	numReaders := flag.Int("num_readers", 0, "Number of readers.")
	fileSize := flag.Int("filesize", 1024, "Number of stripes in each file.")
	driver := flag.String("driver", "roach", "Driver to use. Accepted values are roach or cass.")

	flag.Parse()

	readerPrefix := "reader."
	writerPrefix := "writer." + string(randutil.RandBytes(5)) + "."
	if *layout {
		*numWriters = *numReaders
		writerPrefix = readerPrefix
		*numReaders = 0
	}

	fs, err := filesys.New(*driver, *drop)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < *numReaders; i++ {
		filename := readerPrefix + fmt.Sprint(i)
		log.Print("Started reader with file ", filename)
		wg.Add(1)
		go fs.Read(readerPrefix+fmt.Sprint(i), *fileSize, &wg)
	}

	for i := 0; i < *numWriters; i++ {
		filename := writerPrefix + fmt.Sprint(i)
		log.Print("Started writer with file ", filename)
		wg.Add(1)
		go fs.Write(filename, *fileSize, &wg)
	}

	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

Loop:
	for {
		select {
		case <-tick:
			fs.PrintStats()
		case <-done:
			fs.PrintStats()
			break Loop
		}
	}

	if !*layout {
		for i := 0; i < *numWriters; i++ {
			fs.Delete(writerPrefix + fmt.Sprint(i))
		}
	}
}
