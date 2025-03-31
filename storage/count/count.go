package count

import (
	"fmt"
	"sync/atomic"
)

type Count struct {
	Gets      atomic.Int64
	Hits      atomic.Int64
	Misses    atomic.Int64
	Puts      atomic.Int64
	GetErrors atomic.Int64
	PutErrors atomic.Int64
}

func (c *Count) Summary(kind string) string {
	getsLine := fmt.Sprintf("[%s] %d gets, %d hits, %d misses, %d errors", kind, c.Gets.Load(), c.Hits.Load(), c.Misses.Load(), c.GetErrors.Load())
	putsLine := fmt.Sprintf("[%s] %d puts, %d errors", kind, c.Puts.Load(), c.PutErrors.Load())

	return fmt.Sprintf("%s\n%s", getsLine, putsLine)
}
