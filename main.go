package main

import (
	"context"
	"flag"
	"github.com/sonatard/gocache/server"
	"github.com/sonatard/gocache/storage"
	"log"
	"os"
	"path/filepath"
)

var (
	cacheDir  = flag.String("dir", "", "cache directory")
	s3Bucket  = flag.String("s3-bucket", "", "Amazon S3 bucket name")
	gcsBucket = flag.String("gcs-bucket", "", "Google CLoud Storage bucket name")
	cacheKey  = flag.String("key", "", "cache key")
	verbose   = flag.Bool("verbose", false, "print detail log")
)

const defaultCacheKey = "v1"

func defaultCacheDir() string {
	d, err := os.UserCacheDir()
	if err != nil {
		log.Fatal(err)
	}

	return filepath.Join(d, "gocache")
}

func main() {
	flag.Parse()
	if *cacheDir == "" {
		*cacheDir = defaultCacheDir()
	}

	if *cacheKey == "" {
		a := defaultCacheKey
		cacheKey = &a
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localStorage := storage.New(ctx, *cacheDir, *s3Bucket, *gcsBucket, *cacheKey, *verbose)
	process := server.NewProcess(localStorage, *verbose)
	if err := process.Run(ctx); err != nil {
		log.Fatal(err)
	}

	if *verbose {
		log.Println(localStorage.Summary())
	}
}
