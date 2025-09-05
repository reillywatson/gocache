package storage

import (
	"context"
	"log"

	"github.com/reillywatson/gocache/storage/local"
	"github.com/reillywatson/gocache/storage/remote"
)

// New creates a new cache instance.
//
// Cache storage option:
// 1. only local disk
// 2. Amazon S3 and local disk
// 3. Google Cloud Storage and local disk
func New(ctx context.Context, cacheDir, s3Bucket, gcsBucket, cacheKey string, verbose bool) local.Storage {
	disk := local.NewDisk(verbose, cacheDir)

	switch {
	case s3Bucket != "":
		s3Client, err := remote.NewAmazonS3Client(ctx)
		if err != nil {
			log.Printf("Warning: Amazon S3 configuration failed: %v", err)
			return disk
		}

		amazonS3 := remote.NewAmazonS3(s3Client, s3Bucket, cacheKey, verbose)
		return local.NewMergeRemote(disk, amazonS3, verbose)

	case gcsBucket != "":
		cloudStorageClient, err := remote.NewGoogleCloudStorageClient(ctx)
		if err != nil {
			log.Printf("Warning: Google Cloud Storage configuration failed: %v", err)
			return disk
		}

		googleCloudStorage := remote.NewGoogleCloudStorage(cloudStorageClient, gcsBucket, cacheKey, verbose)
		return local.NewMergeRemote(disk, googleCloudStorage, verbose)
	default:
		return disk
	}
}
