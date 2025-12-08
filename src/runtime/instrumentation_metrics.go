package runtime

import "runtime/internal/atomic"

const MaxEvents = 1 << 18
const WAIT_REASON_NOOP = 66
const STATUS_NOOP = 66

// type ActionCode int

const (
	GOROUTINE_CREATION int = iota
	LOCAL_QUEUE_TAIL
	GLOBAL_QUEUE_PUSH
	LOCAL_QUEUE_HEAD
	GLOBAL_TO_LOCAL
	LOCAL_QUEUE_POP
	LOCAL_QUEUE_DRAIN
	PROCESSOR_WORK_STEAL
	GOROUTINE_EXECUTION
	GOROUTINE_READY
	GOROUTINE_IDLE
	GOROUTINE_CHANGE_STATUS
)

var ActionIDStrings = map[int]string{
	GOROUTINE_CREATION:      "Goroutine Created",
	LOCAL_QUEUE_TAIL:        "Goroutine Pushed To Tail of Local Queue",
	GLOBAL_QUEUE_PUSH:       "Goroutine Pushed To Global Queue",
	LOCAL_QUEUE_HEAD:        "Goroutine Pushed To Head of Local Queue",
	GLOBAL_TO_LOCAL:         "Goroutine Pushed from Global to Local Queue",
	LOCAL_QUEUE_POP:         "Goroutine Popped from Local Queue to Run",
	LOCAL_QUEUE_DRAIN:       "Goroutine Drained and Flushed",
	PROCESSOR_WORK_STEAL:    "Goroutine Was Stolen From Its Previous Processor",
	GOROUTINE_EXECUTION:     "Goroutine Was Executed to Perform A Task",
	GOROUTINE_READY:         "Goroutine set to Ready",
	GOROUTINE_IDLE:          "Goroutine set to Idle",
	GOROUTINE_CHANGE_STATUS: "Goroutine changed status",
}

type gstatus uint32

const (
	GIDLE gstatus = iota
	GRUNNABLE
	GRUNNING
	GSYSCALL
	GWAITING
	GMORIBUND_UNUSED
	GDEAD
	GENQUEUE_UNUSED
	GCOPYSTACK
	GPREEMPTED
	GSCAN
)

var GoroutineStatusStrings = map[gstatus]string{
	GIDLE:            "_GIdle: Just allocated, not initialized",
	GRUNNABLE:        "_Grunnable: On a run queue",
	GRUNNING:         "_Grunning: Running User Code",
	GSYSCALL:         "_Gsyscall: Running System Call Code",
	GWAITING:         "_Gwaiting: Blocked in Runtime",
	GMORIBUND_UNUSED: "__Gmoribund_unused: Illegal",
	GDEAD:            "_Gdead: Currently Unsused",
	GENQUEUE_UNUSED:  "_Genqueue_unused: Illegal",
	GCOPYSTACK:       "_Gcopystack: Stack being Moved",
	GPREEMPTED:       "_Gpreempted: Stopped itself for preemption routine",
	GSCAN:            "_Gscan: GC Scanning the stack",
}

type SchedEvent struct {
	Timestamp   int64 // timestamp (nanoseconds)
	ActionID    int
	GoRoutineID int64 // goroutine ID, ID:0 is the scheduler
	ProcessorID int32 // processor ID
}

type ChangeEvent struct {
	Timestamp   int64 // timestamp (nanoseconds)
	ActionID    int
	GoRoutineID int64  // goroutine ID, ID:0 is the scheduler
	ProcessorID int32  // processor ID
	OldStatus   uint32 // the status this goroutine moved from, 66 is a no-op (invalid)
	NewStatus   uint32 // the status this goroutine moved from to, 66 is a no-op (invalid)
	WaitReason  uint8  // (waitReason) Reason why the gorouine was put to wait if relevant action, 66 is a no-op (invalid)
}

type GQueueTimestamp struct {
	Timestamp   int64 // timestamp (nanoseconds)
	ProcessorID int32 // processor ID, ID: -1 is the scheduler so we can measure the global queue
	QSize       int32 // number of gorountines the runq holds at this time
}

var (
	GoEvents           [MaxEvents]SchedEvent
	GoEventIdx         uint64
	QueueLenTimestamps [MaxEvents]GQueueTimestamp
	QSizeIdx           uint64
	ChangeStatusEvents [MaxEvents]ChangeEvent
	ChangeStatusIdx    uint64
	// headPushEvents [MaxEvents]SchedEvent
	// headPushEventIdx uint64
	// globalPushEvents [MaxEvents]SchedEvent
	// globalPushEventIdx uint64
	// g2lPushEvents [MaxEvents]SchedEvent
	// g2lPushEventIdx uint64
	// lpopPushEvents [MaxEvents]SchedEvent
	// lpopPushEventIdx uint64
	// drainPushEvents [MaxEvents]SchedEvent
	// drainPushEventIdx uint64
	// stealPushEvents [MaxEvents]SchedEvent
	// stealPushEventIdx uint64
	// executePushEvents [MaxEvents]SchedEvent
	// executePushEventIdx uint64
)

func log_goroutine_creation(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(GOROUTINE_CREATION, gp, pid, log_entry)
	}
}

func log_goroutine_local_tail_push(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(LOCAL_QUEUE_TAIL, gp, pid, log_entry)
	}
}

func log_goroutine_local_head_push(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(LOCAL_QUEUE_HEAD, gp, pid, log_entry)
	}
}

func log_goroutine_local_global_push(gp *g) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if gp.m != nil {
			pid = int32(gp.m.id)
		}
		log_entry := &GoEvents[idx]
		log_event(GLOBAL_QUEUE_PUSH, gp, pid, log_entry)
	}
}

func log_goroutine_global_to_local(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(GLOBAL_TO_LOCAL, gp, pid, log_entry)
	}
}

func log_goroutine_local_pop(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(LOCAL_QUEUE_POP, gp, pid, log_entry)
	}
}

func log_goroutine_local_drain(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(LOCAL_QUEUE_DRAIN, gp, pid, log_entry)
	}
}

func log_goroutine_local_steal(gp *g, pp *p) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		if pp != nil {
			pid = pp.id
		}
		log_entry := &GoEvents[idx]
		log_event(PROCESSOR_WORK_STEAL, gp, pid, log_entry)
	}
}

func log_goroutine_execution(gp *g) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	if idx < MaxEvents {
		pid := int32(-1)
		log_entry := &GoEvents[idx]
		log_event(GOROUTINE_EXECUTION, gp, pid, log_entry)
	}
}

func log_goroutine_change_status(gp *g, oldval, newval uint32) {
	idx := atomic.Xadd64(&GoEventIdx, 1) - 1
	var wr uint8
	wr = 66
	if gstatus(newval) == GWAITING {
		wr = uint8(gp.waitreason)
	}
	if idx < MaxEvents {
		pid := int32(-1)
		if gp.m != nil {
			pid = int32(gp.m.id)
		}
		log_entry := &ChangeStatusEvents[idx]
		log_change_stat_event(GOROUTINE_CHANGE_STATUS, gp, pid, log_entry, oldval, newval, wr)
	}
}

func log_q_size(goid int32, size int32) {
	idx := atomic.Xadd64(&QSizeIdx, 1) - 1
	if idx < MaxEvents {
		log_entry := &QueueLenTimestamps[idx]
		log_entry.ProcessorID = goid
		log_entry.QSize = size
		log_entry.Timestamp = nanotime()
	}
}

func log_event(actionID int, gp *g, pid int32, log_entry *SchedEvent) {
	log_entry.Timestamp = nanotime()
	log_entry.GoRoutineID = int64(gp.goid)
	log_entry.ActionID = actionID
	log_entry.ProcessorID = pid
}

func log_change_stat_event(actionID int, gp *g, pid int32, log_entry *ChangeEvent, oldval, newval uint32, waitReason uint8) {
	log_entry.Timestamp = nanotime()
	log_entry.GoRoutineID = int64(gp.goid)
	log_entry.ActionID = actionID
	log_entry.ProcessorID = pid
	log_entry.OldStatus = oldval
	log_entry.NewStatus = newval
	log_entry.WaitReason = waitReason
}

func Dump_instrumentation_logs() {
	if !instrumentationEnabled {
		print("Instrumentation disabled\n")
		return
	}

	print("=== Instrumentation Dump ===\n")
	max := GoEventIdx
	if GoEventIdx > MaxEvents {
		max = MaxEvents
	}

	for i := uint64(0); i < max; i++ {
		e := GoEvents[i]
		print("Time: ", e.Timestamp, " - Goroutine ", e.GoRoutineID, " action: ", ActionIDStrings[e.ActionID])
		if e.ActionID == PROCESSOR_WORK_STEAL {
			print(", Stolen from Processor P", e.ProcessorID)
		}
		if !(e.ActionID == GOROUTINE_EXECUTION) && !(e.ActionID == GOROUTINE_READY) && !(e.ActionID == PROCESSOR_WORK_STEAL) && !(e.ActionID == GLOBAL_QUEUE_PUSH) && !(e.ActionID == GOROUTINE_CHANGE_STATUS) {
			print(", ran on P", e.ProcessorID)
		}
		print("\n")
	}
	print("Total # events: ", max, "\n")

	print("=== End Dump ===\n")
}

func Dump_change_status_logs() {
	if !instrumentationEnabled {
		print("Instrumentation disabled\n")
		return
	}

	max := ChangeStatusIdx
	if ChangeStatusIdx > MaxEvents {
		max = MaxEvents
	}

	print("=== Goroutine Status Log Dump ===\n")

	for i := uint64(0); i < max; i++ {
		slog := ChangeStatusEvents[i]
		newStat := gstatus(slog.NewStatus)
		print("Time: ", slog.Timestamp, " - Goroutine ", slog.GoRoutineID, " action: ", ActionIDStrings[slog.ActionID])
		print(", From: ", gStatusStrings[gstatus(slog.OldStatus)], " To: ", gStatusStrings[newStat])
		if newStat == GWAITING {
			print(", Waiting Reason: ", waitReasonStrings[waitReason(slog.WaitReason)])
		}
		print("\n")
	}

	print("Total # events: ", max, "\n")
	print("=== End Dump ===\n")

}

func Dump_qsize_logs() {
	if !instrumentationEnabled {
		print("Instrumentation disabled\n")
		return
	}

	max := QSizeIdx
	if QSizeIdx > MaxEvents {
		max = MaxEvents
	}

	print("=== QSize Log Dump ===\n")

	for i := uint64(0); i < max; i++ {
		t := QueueLenTimestamps[i]
		print("Timestamp: ", t.Timestamp, "\tProcessorID: ", t.ProcessorID, "\tQueue Size: ", t.QSize, "\n")
	}

	print("Total # events: ", max, "\n")
	print("=== End Dump ===\n")
}
