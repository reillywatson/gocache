# gocache

[![GoDoc](https://pkg.go.dev/badge/github.com/reillywatson/gocache)](https://pkg.go.dev/github.com/reillywatson/gocache)

gocache is a tool to manage go build cache.

> Package cacheprog defines the protocol for a GOCACHEPROG program.
> By default, the go command manages a build cache stored in the file system itself. GOCACHEPROG can be set to the name of a command (with optional space-separated flags) that implements the go command build cache externally. This permits defining a different cache policy.
> The go command will start the GOCACHEPROG as a subprocess and communicate with it via JSON messages over stdin/stdout. The subprocess's stderr will be connected to the go command's stderr.
> https://pkg.go.dev/cmd/go/internal/cacheprog

## Using

```sh
$ go get -tool github.com/reillywatson/gocache@latest
$ GOCACHEPROG="go tool gocache" go install std
```

or

```
$ go install github.com/reillywatson/gocache@latest
$ GOCACHEPROG="gocache" go install std
```

## Options

### --verbose

```sh
$ GOCACHEPROG="go tool gocache --verbose" go install std
[disk] local cache in /Users/xxx/Library/Caches/gocache
[gcs] start to gs://yyyy/cache/v1/arm64/darwin/go1.24.1
[gcs] put success for gs://yyyy/cache/v1/arm64/darwin/go1.24.1/zzz (outputID: aaa, size: 415)
```

### --cache-dir

```sh
$ GOCACHEPROG="go tool gocache --verbose --cache-dir=${HOME}/cache/" go install std
[disk] local cache in /Users/xxx/cache
```


### --s3-bucket
Amazon S3 Bucket

```sh
$ GOCACHEPROG="go tool gocache --verbose --s3-bucket=yyyy" go install std
```

- Authentication: https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/config#LoadDefaultConfig
- storage path: `s3://<bucket>/cache/<cache_key>/<architecture>/<os>/<go-version>`

### --gcs-bucket
Google Cloud Storage Bucket

```sh
$ GOCACHEPROG="go tool gocache --verbose --gcs-bucket=yyyy" go install std
```
- Authentication: https://pkg.go.dev/cloud.google.com/go/storage#NewClient
- storage path: `gs://<bucket>/cache/<cache_key>/<architecture>/<os>/<go-version>`
