package runtime

import "runtime/internal/atomic"

const maxEvents = 1 << 16 // 65,536 events

type schedEvent struct {
    Timestamp   	int64  // timestamp (nanoseconds)
    GoRoutineID  	int64  // goroutine ID
    ProcessorID     int32  // processor ID
}

var (
    schedEvents [maxEvents]schedEvent
    schedEventIdx uint64
)

func log_goroutine_creation(gp *g){
	idx := atomic.Xadd64(&schedEventIdx, 1) - 1
    if idx < maxEvents {
        e := &schedEvents[idx]
        e.Timestamp = nanotime()
        e.GoRoutineID = int64(gp.goid)

		// gp.m could be nil in early boot, gp.m.p could be nil
		if gp.m != nil {
			e.ProcessorID = int32(gp.m.id)
		} else {
			e.ProcessorID = -1
		}
    }
}
