package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

//go:generate go run github.com/gqgs/argsgen@latest

type options struct {
	db          string `arg:"database name (sqlite),required"`
	bucket      string `arg:"S3 bucket to mount,required"`
	mountpoint  string `arg:"mountpoint for bucket,required"`
	debug       bool   `arg:"enable debug mode"`
	verbose     bool   `arg:"enable verbose debug mode"`
	concurrency int    `arg:"concurrency level for upload/download"`
	profile     bool   `arg:"create CPU profile"`
}

func main() {
	o := options{
		db:          os.Getenv("S3FS_DB"),
		bucket:      os.Getenv("S3FS_BUCKET"),
		mountpoint:  os.Getenv("S3FS_MOUNTPOINT"),
		concurrency: runtime.GOMAXPROCS(0),
	}
	o.MustParse()

	if o.debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if o.profile {
		f, err := os.Create("default.pgo")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			slog.Error("error creating CPU profile", "err", err)
		}

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			pprof.StopCPUProfile()
			os.Exit(1)
		}()
	}

	if err := handler(o); err != nil {
		slog.Error(err.Error())
	}
}
