package local

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"golang.org/x/sync/errgroup"

	"github.com/sonatard/gocache/storage/remote"
)

// MergeRemote is a storage that is backed by a local storage and a remote storage.
type MergeRemote struct {
	localStorage  Storage
	remoteStorage remote.Storage
	verbose       bool
}

var _ Storage = &MergeRemote{}

func NewMergeRemote(localStorage Storage, remoteStorage remote.Storage, verbose bool) *MergeRemote {
	return &MergeRemote{
		localStorage:  localStorage,
		remoteStorage: remoteStorage,
		verbose:       verbose,
	}
}

func (m *MergeRemote) Kind() string {
	return "merge-remote"
}

func (m *MergeRemote) Start(ctx context.Context) error {
	err := m.localStorage.Start(ctx)
	if err != nil {
		_ = m.localStorage.Close()
		return fmt.Errorf("local cache start failed: %w", err)
	}
	err = m.remoteStorage.Start(ctx)
	if err != nil {
		_ = m.remoteStorage.Close()
		return fmt.Errorf("remote cache start failed: %w", err)
	}

	return nil
}

func (m *MergeRemote) Get(ctx context.Context, actionID string) (string, string, error) {
	localOutputID, localDiskPath, localErr := m.localStorage.Get(ctx, actionID)
	if localErr == nil && localOutputID != "" {
		return localOutputID, localDiskPath, nil
	}

	remoteOutputID, _, _, remoteErr := m.remoteStorage.Get(ctx, actionID)
	if remoteErr == nil && remoteOutputID != "" {
		return remoteOutputID, localDiskPath, nil
	}

	// not found
	return "", "", cmp.Or(localErr, remoteErr)
}

// Put writes the data to the remote cache and local cache.
func (m *MergeRemote) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error) {
	// 2. write pw from remote cache in remoteStorage.Put and then read it from pr, write it to local disk in localStorage.Put.
	pr, pw := io.Pipe()

	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewReader(nil)
		}
		var localErr error
		diskPath, localErr = m.localStorage.Put(ctx, actionID, outputID, size, putBody)
		return localErr
	})

	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {
		// 1. read putBody in remoteStorage.Put and then write to pw.
		putBody = io.TeeReader(body, pw)
	}

	if err := m.remoteStorage.Put(ctx, actionID, outputID, size, putBody); err != nil {
		return "", err
	}

	_ = pw.Close()
	if err := wg.Wait(); err != nil {
		log.Printf("[%s] error: %v", m.localStorage.Kind(), err)
		return "", err
	}
	return diskPath, nil

}

func (m *MergeRemote) Close() error {
	var errAll error
	if err := m.localStorage.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("local cache close failed: %w", err), errAll)
	}
	if err := m.remoteStorage.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("remote cache close failed: %w", err), errAll)
	}

	return errAll
}

func (m *MergeRemote) Summary() string {
	return fmt.Sprintf("\n%s\n%s", m.localStorage.Summary(), m.remoteStorage.Summary())
}
