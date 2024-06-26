package main

import (
	"log/slog"
	"os"
	"runtime"
)

//go:generate go run github.com/gqgs/argsgen@latest

type options struct {
	db          string `arg:"database name (sqlite),required"`
	bucket      string `arg:"S3 bucket to mount,required"`
	mountpoint  string `arg:"mountpoint for bucket,required"`
	debug       bool   `arg:"enable debug mode"`
	verbose     bool   `arg:"enable verbose debug mode"`
	concurrency int    `arg:"concurrency level for upload/download"`
}

func main() {
	o := options{
		db:          os.Getenv("S3FS_DB"),
		bucket:      os.Getenv("S3FS_BUCKET"),
		mountpoint:  os.Getenv("S3FS_MOUNTPOINT"),
		concurrency: runtime.GOMAXPROCS(0),
	}
	o.MustParse()

	if o.debug || o.verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if err := handler(o); err != nil {
		slog.Error(err.Error())
	}
}
