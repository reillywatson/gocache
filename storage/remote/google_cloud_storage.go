package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"

	"github.com/sonatard/gocache/storage/count"

	"cloud.google.com/go/storage"
)

func NewGoogleCloudStorageClient(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Cloud Storage client: %w", err)
	}
	return client, nil

}

var _ Storage = &GoogleCloudStorage{}

// GoogleCloudStorage is a remote cache that is backed by a Google Cloud Storage bucket
type GoogleCloudStorage struct {
	client     *storage.Client
	bucketName string
	bucket     *storage.BucketHandle
	bucketPath string
	verbose    bool
	count.Count
}

// NewGoogleCloudStorage creates a new GoogleCloudStorage instance.
func NewGoogleCloudStorage(client *storage.Client, bucketName string, cacheKey string, verbose bool) *GoogleCloudStorage {
	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}
	goVersion := os.Getenv("GOVERSION")
	if goVersion == "" {
		goVersion = runtime.Version()
	}

	bucketPath := path.Join("cache", cacheKey, goarch, goos, goVersion)

	return &GoogleCloudStorage{
		client:     client,
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		bucketPath: bucketPath,
		verbose:    verbose,
	}
}

func (g *GoogleCloudStorage) objectName(actionID string) string {
	return fmt.Sprintf("%s/%s", g.bucketPath, actionID)
}

func (g *GoogleCloudStorage) bucketFullPath() string {
	return fmt.Sprintf("gs://%s/%s", g.bucketName, g.bucketPath)
}

func (g *GoogleCloudStorage) Kind() string {
	return "gcs"
}

func (g *GoogleCloudStorage) Start(ctx context.Context) error {
	if g.verbose {
		log.Printf("[%s] start to %s", g.Kind(), g.bucketFullPath())
	}
	if _, err := g.bucket.Attrs(ctx); err != nil {
		return fmt.Errorf("[%s] failed to start %s: %w", g.Kind(), g.bucketFullPath(), err)
	}
	return nil
}

func (g *GoogleCloudStorage) Get(ctx context.Context, actionID string) (string, int64, io.ReadCloser, error) {
	g.Count.Gets.Add(1)
	objectName := g.objectName(actionID)
	obj := g.bucket.Object(objectName)

	attrs, err := obj.Attrs(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		g.Count.Misses.Add(1)
		return "", 0, nil, nil
	}
	if err != nil {
		g.Count.GetErrors.Add(1)
		return "", 0, nil, fmt.Errorf("[%s] get %s/%s (%v)", g.Kind(), g.bucketFullPath(), actionID, err)
	}

	outputID, ok := attrs.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		g.Count.GetErrors.Add(1)
		return "", 0, nil, fmt.Errorf("[%s] get %s/%s (outputID not found in GCS metadata for object)", g.Kind(), g.bucketFullPath(), actionID)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		g.Count.GetErrors.Add(1)
		return "", 0, nil, fmt.Errorf("[%s] get %s/%s (failed to create GCS reader): %w", g.Kind(), g.bucketFullPath(), actionID, err)
	}

	if g.verbose {
		log.Printf("[%s] get success %s/%s (size: %v)", g.Kind(), g.bucketFullPath(), actionID, attrs.Size)
	}
	return outputID, attrs.Size, reader, nil
}

func (g *GoogleCloudStorage) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	g.Count.Puts.Add(1)
	objectName := g.objectName(actionID)
	obj := g.bucket.Object(objectName)
	writer := obj.NewWriter(ctx)
	defer writer.Close()

	writer.Metadata = map[string]string{
		outputIDMetadataKey: outputID,
	}
	writer.Size = size

	if _, err = io.Copy(writer, body); err != nil {
		g.Count.PutErrors.Add(1)
		return fmt.Errorf("[%s] put failed for %s/%s (failed to copy data to GCS outputID: %s, size: %d): %w", g.Kind(), g.bucketFullPath(), actionID, outputID, size, err)
	}

	if g.verbose {
		log.Printf("[%s] put success for %s/%s (outputID: %s, size: %d)", g.Kind(), g.bucketFullPath(), outputID, actionID, size)
	}

	return nil
}

func (g *GoogleCloudStorage) Close() error {
	err := g.client.Close()
	if err != nil {
		return fmt.Errorf("[%s] close %s (error: %v)", g.Kind(), g.bucketFullPath(), err)
	}
	if g.verbose {
		log.Printf("[%s] close success %s", g.Kind(), g.bucketFullPath())
	}
	return nil
}

func (g *GoogleCloudStorage) Summary() string {
	return g.Count.Summary(g.Kind())
}
