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

	"github.com/reillywatson/gocache/storage/count"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func NewAmazonS3Client(ctx context.Context) (*s3.Client, error) {
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return s3.NewFromConfig(awsConfig), nil
}

var _ Storage = &AmazonS3{}

// AmazonS3 is a remote cache that is backed by Amazon S3 bucket
type AmazonS3 struct {
	s3Client   *s3.Client
	bucket     string
	bucketPath string
	verbose    bool
	count.Count
}

func NewAmazonS3(client *s3.Client, bucketName string, cacheKey string, verbose bool) *AmazonS3 {
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
	return &AmazonS3{
		s3Client:   client,
		bucket:     bucketName,
		bucketPath: bucketPath,
		verbose:    verbose,
	}
}

func (a *AmazonS3) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", a.bucketPath, actionID)
}

func (a *AmazonS3) Kind() string {
	return "s3"
}

func (a *AmazonS3) Start(context.Context) error {
	if a.verbose {
		log.Printf("[%s] configured to s3://%s/%s", a.Kind(), a.bucket, a.bucketPath)
	}
	return nil
}

func (a *AmazonS3) Get(ctx context.Context, actionID string) (string, int64, io.ReadCloser, error) {
	a.Count.Gets.Add(1)
	actionKey := a.actionKey(actionID)
	getObjectOutput, err := a.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &a.bucket,
		Key:    &actionKey,
	})
	if isNotFoundError(err) {
		a.Count.Misses.Add(1)
		return "", 0, nil, nil
	}
	if err != nil {
		a.Count.GetErrors.Add(1)
		return "", 0, nil, fmt.Errorf("[%s] get %s/%s (%v)", a.Kind(), a.bucket, actionKey, err)
	}
	contentSize := getObjectOutput.ContentLength
	outputID, ok := getObjectOutput.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		a.Count.GetErrors.Add(1)
		return "", 0, nil, fmt.Errorf("[%s] get %s/%s (outputID not found in S3 metadata for object)", a.Kind(), a.bucket, actionKey)
	}
	return outputID, *contentSize, getObjectOutput.Body, nil
}

func (a *AmazonS3) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) error {
	a.Count.Puts.Add(1)
	actionKey := a.actionKey(actionID)
	if _, err := a.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &a.bucket,
		Key:           &actionKey,
		Body:          body,
		ContentLength: &size,
		Metadata: map[string]string{
			outputIDMetadataKey: outputID,
		},
	}, func(options *s3.Options) {
		options.RetryMaxAttempts = 1 // We cannot perform seek in Body
	}); err != nil {
		a.Count.PutErrors.Add(1)
		return fmt.Errorf("[%s] put failed for %s/%s (failed to put object to S3 outputID: %s, size: %d)", a.Kind(), a.bucket, actionKey, outputID, size)
	}

	return nil
}

func (a *AmazonS3) Close() error {
	return nil
}

func (a *AmazonS3) Summary() string {
	return a.Count.Summary(a.Kind())
}

func isNotFoundError(err error) bool {
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			return code == "AccessDenied" || code == "NoSuchKey"
		}
	}
	return false
}
