package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/reillywatson/gocache/storage/local"

	"golang.org/x/sync/errgroup"

	"github.com/reillywatson/gocache/server/internal/cacheprog"
)

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrNoOutputID     = errors.New("no outputID")
)

type Process struct {
	cache    local.Storage
	closer   sync.Once
	errClose error
	verbose  bool
}

func NewProcess(cache local.Storage, verbose bool) *Process {
	return &Process{
		cache:   cache,
		verbose: verbose,
	}
}

func (p *Process) Run(ctx context.Context) error {
	br := bufio.NewReader(os.Stdin)
	jd := json.NewDecoder(br)

	bw := bufio.NewWriter(os.Stdout)
	je := json.NewEncoder(bw)
	caps := []cacheprog.Cmd{cacheprog.CmdGet, cacheprog.CmdPut, cacheprog.CmdClose}
	if err := je.Encode(&cacheprog.Response{KnownCommands: caps}); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}

	// guards writing responses
	var wmu sync.Mutex

	wg, ctx := errgroup.WithContext(ctx)
	if err := p.cache.Start(ctx); err != nil {
		return err
	}
	defer func() {
		_ = p.close()
		_ = wg.Wait()
	}()
	for {
		req, err := p.parseRequest(jd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		wg.Go(func() error {
			res := &cacheprog.Response{ID: req.ID}
			if err := p.handleRequest(ctx, req, res); err != nil {
				res.Err = err.Error()
			}
			wmu.Lock()
			defer wmu.Unlock()
			_ = je.Encode(res)
			_ = bw.Flush()
			return nil
		})
	}
}

func (p *Process) parseRequest(jd *json.Decoder) (*cacheprog.Request, error) {
	var req cacheprog.Request
	if err := jd.Decode(&req); err != nil {
		return nil, err
	}
	if req.Command == cacheprog.CmdPut {
		if req.BodySize > 0 {
			// io.Reader that validates on EOF.
			var bodyb []byte
			if err := jd.Decode(&bodyb); err != nil {
				return nil, err
			}
			if int64(len(bodyb)) != req.BodySize {
				return nil, fmt.Errorf("only got %d bytes of declared %d", len(bodyb), req.BodySize)
			}
			req.Body = bytes.NewReader(bodyb)
		}
	}

	return &req, nil
}

func (p *Process) handleRequest(ctx context.Context, req *cacheprog.Request, res *cacheprog.Response) error {
	var err error
	switch req.Command {
	case cacheprog.CmdGet:
		err = p.handleGet(ctx, req, res)
	case cacheprog.CmdPut:
		err = p.handlePut(ctx, req, res)
	case cacheprog.CmdClose:
		err = p.close()
	default:
		return ErrUnknownCommand
	}
	if p.verbose {
		if err != nil {
			log.Printf("%v", err)
		}
	}
	return err
}

func (p *Process) handleGet(ctx context.Context, req *cacheprog.Request, res *cacheprog.Response) (retErr error) {
	outputID, diskPath, err := p.cache.Get(ctx, fmt.Sprintf("%x", req.ActionID))
	if err != nil {
		return err
	}
	if outputID == "" && diskPath == "" {
		res.Miss = true
		return nil
	}
	if outputID == "" {
		return ErrNoOutputID
	}
	res.OutputID, err = hex.DecodeString(outputID)
	if err != nil {
		return fmt.Errorf("invalid OutputID: %w", err)
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			res.Miss = true
			return nil
		}
		return err
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	res.Size = fi.Size()
	res.DiskPath = diskPath
	return nil
}

func (p *Process) handlePut(ctx context.Context, req *cacheprog.Request, res *cacheprog.Response) (retErr error) {
	actionID, outputID := fmt.Sprintf("%x", req.ActionID), fmt.Sprintf("%x", req.OutputID)
	defer func() {
		if retErr != nil {
			log.Printf("put(action %s, obj %s, %v bytes): %v", actionID, outputID, req.BodySize, retErr)
		}
	}()
	var body = req.Body
	if body == nil {
		body = bytes.NewReader(nil)
	}
	diskPath, err := p.cache.Put(ctx, actionID, outputID, req.BodySize, body)
	if err != nil {
		return err
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		return fmt.Errorf("stat after successful Put: %w", err)
	}
	if fi.Size() != req.BodySize {
		return fmt.Errorf("failed to write file to disk with right size: disk=%v; wanted=%v", fi.Size(), req.BodySize)
	}
	res.DiskPath = diskPath
	return nil
}

func (p *Process) close() error {
	p.closer.Do(func() {
		p.errClose = p.cache.Close()
		if p.errClose != nil {
			log.Printf("cache stop failed: %v", p.errClose)
		}
	})
	return p.errClose
}
