package local

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/sonatard/gocache/storage/count"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// indexEntry is the metadata that Disk stores on disk for an ActionID.
type indexEntry struct {
	Version   int    `json:"v"`
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeNanos int64  `json:"t"`
}

var _ Storage = &Disk{}

// Disk is a Local that stores data on disk.
type Disk struct {
	dir     string
	verbose bool
	count.Count
}

func NewDisk(verbose bool, dir string) *Disk {
	return &Disk{
		dir:     dir,
		verbose: verbose,
	}
}

func (d *Disk) Kind() string {
	return "disk"
}

func (d *Disk) Start(context.Context) error {
	if d.verbose {
		log.Printf("[%s] local cache in %s", d.Kind(), d.dir)
	}
	return os.MkdirAll(d.dir, 0755)
}

func (d *Disk) Get(_ context.Context, actionID string) (outputID, diskPath string, err error) {
	d.Count.Gets.Add(1)
	actionFile := filepath.Join(d.dir, fmt.Sprintf("a-%s", actionID))
	ij, err := os.ReadFile(actionFile)
	if os.IsNotExist(err) {
		d.Count.Misses.Add(1)
		return "", "", nil
	}
	if err != nil {
		d.Count.GetErrors.Add(1)
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		log.Printf("Warning: JSON error for action %q: %v", actionID, err)
		return "", "", nil
	}
	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		// Protect against malicious non-hex OutputID on disk
		return "", "", nil
	}
	return ie.OutputID, filepath.Join(d.dir, fmt.Sprintf("o-%v", ie.OutputID)), nil
}

func (d *Disk) Put(_ context.Context, actionID, objectID string, size int64, body io.Reader) (diskPath string, _ error) {
	d.Count.Puts.Add(1)
	file := filepath.Join(d.dir, fmt.Sprintf("o-%s", objectID))

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			d.Count.PutErrors.Add(1)
			return "", err
		}
		_ = zf.Close()
	} else {
		wrote, err := writeAtomic(file, body)
		if err != nil {
			d.Count.PutErrors.Add(1)
			return "", err
		}
		if wrote != size {
			d.Count.PutErrors.Add(1)
			return "", fmt.Errorf("wrote %d bytes, expected %d", wrote, size)
		}
	}

	ij, err := json.Marshal(indexEntry{
		Version:   1,
		OutputID:  objectID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		d.Count.PutErrors.Add(1)
		return "", err
	}
	actionFile := filepath.Join(d.dir, fmt.Sprintf("a-%s", actionID))
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		d.Count.PutErrors.Add(1)
		return "", err
	}
	return file, nil
}

func (d *Disk) Close() error {
	return nil
}

func (d *Disk) Summary() string {
	return d.Count.Summary(d.Kind())
}

func writeTempFile(dest string, r io.Reader) (string, int64, error) {
	tf, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".*")
	if err != nil {
		return "", 0, err
	}
	fileName := tf.Name()
	defer func() {
		_ = tf.Close()
		if err != nil {
			_ = os.Remove(fileName)
		}
	}()
	size, err := io.Copy(tf, r)
	if err != nil {
		return "", 0, err
	}
	return fileName, size, nil
}

func writeAtomic(dest string, r io.Reader) (int64, error) {
	tempFile, size, err := writeTempFile(dest, r)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tempFile)
		}
	}()
	if err = os.Rename(tempFile, dest); err != nil {
		return 0, err
	}
	return size, nil
}
