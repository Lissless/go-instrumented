package runtime

import "runtime/internal/atomic"

const maxEvents = 1 << 16 // 65,536 events

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
)

var ActionIDStrings = map[int]string{
    GOROUTINE_CREATION: "Goroutine Created",
    LOCAL_QUEUE_TAIL: "Goroutine Pushed To Tail of Local Queue",
    GLOBAL_QUEUE_PUSH: "Goroutine Pushed To Global Queue",
    LOCAL_QUEUE_HEAD: "Goroutine Pushed To Head of Local Queue",
    GLOBAL_TO_LOCAL: "Goroutine Pushed from Global to Local Queue",
    LOCAL_QUEUE_POP: "Goroutine Popped from Local Queue to Run",
    LOCAL_QUEUE_DRAIN: "Goroutine Drained and Flushed",
    PROCESSOR_WORK_STEAL: "Goroutine Was Stolen From Its Previous Processor",
    GOROUTINE_EXECUTION: "Goroutine Was Executed to Perform A Task",
}

type schedEvent struct {
    ActionID        int
    Timestamp   	int64  // timestamp (nanoseconds)
    GoRoutineID  	int64  // goroutine ID
    ProcessorID     int32  // processor ID
}

var (
    goCreations [maxEvents]schedEvent
    goCreationIdx uint64
    tailPushEvents [maxEvents]schedEvent
    tailPushEventIdx uint64
    headPushEvents [maxEvents]schedEvent
    headPushEventIdx uint64
    globalPushEvents [maxEvents]schedEvent
    globalPushEventIdx uint64
    g2lPushEvents [maxEvents]schedEvent
    g2lPushEventIdx uint64
    lpopPushEvents [maxEvents]schedEvent
    lpopPushEventIdx uint64
    drainPushEvents [maxEvents]schedEvent
    drainPushEventIdx uint64
    stealPushEvents [maxEvents]schedEvent
    stealPushEventIdx uint64
    executePushEvents [maxEvents]schedEvent
    executePushEventIdx uint64
)

func log_goroutine_creation(gp *g){
	idx := atomic.Xadd64(&goCreationIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        creation_log := &goCreations[idx]
        log_event(GOROUTINE_CREATION, gp, pid, creation_log);
    }
}

func log_goroutine_local_tail_push(gp *g, pp *p){
	idx := atomic.Xadd64(&tailPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        tail_push_log := &tailPushEvents[idx]
        log_event(LOCAL_QUEUE_TAIL, gp, pid, tail_push_log);
    }
}

func log_goroutine_local_head_push(gp *g, pp *p){
	idx := atomic.Xadd64(&headPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        head_push_log := &headPushEvents[idx]
        log_event(LOCAL_QUEUE_HEAD, gp, pid, head_push_log);
    }
}

func log_goroutine_local_global_push(gp *g){
	idx := atomic.Xadd64(&globalPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        global_push_log := &globalPushEvents[idx]
        log_event(GLOBAL_QUEUE_PUSH, gp, pid, global_push_log);
    }
}

func log_goroutine_global_to_local(gp *g, pp *p){
	idx := atomic.Xadd64(&g2lPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        g2l_push_log := &g2lPushEvents[idx]
        log_event(GLOBAL_TO_LOCAL, gp, pid, g2l_push_log);
    }
}

func log_goroutine_local_pop(gp *g, pp *p){
	idx := atomic.Xadd64(&lpopPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        lpop_push_log := &lpopPushEvents[idx]
        log_event(LOCAL_QUEUE_POP, gp, pid, lpop_push_log);
    }
}

func log_goroutine_local_drain(gp *g, pp *p){
	idx := atomic.Xadd64(&drainPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        drain_push_log := &drainPushEvents[idx]
        log_event(LOCAL_QUEUE_DRAIN, gp, pid, drain_push_log);
    }
}

func log_goroutine_local_steal(gp *g, pp *p){
	idx := atomic.Xadd64(&stealPushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        steal_push_log := &stealPushEvents[idx]
        log_event(PROCESSOR_WORK_STEAL, gp, pid, steal_push_log);
    }
}

func log_goroutine_execution(gp *g){
	idx := atomic.Xadd64(&executePushEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        creation_log := &executePushEvents[idx]
        log_event(GOROUTINE_EXECUTION, gp, pid, creation_log);
    }
}

func log_event(actionID int, gp *g, pid int32, log_entry *schedEvent){
    log_entry.Timestamp = nanotime()
    log_entry.GoRoutineID = int64(gp.goid)
    log_entry.ActionID = actionID
    log_entry.ProcessorID = pid
}

func dump_instrumentation_logs() {
    if !instrumentationEnabled {
        print("Instrumentation disabled\n")
        return
    }

    print("=== Instrumentation Dump ===\n")
    for i := uint64(0); i < goCreationIdx; i++ {
        e := goCreations[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < tailPushEventIdx; i++ {
        e := tailPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < headPushEventIdx; i++ {
        e := headPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < globalPushEventIdx; i++ {
        e := globalPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < g2lPushEventIdx; i++ {
        e := g2lPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < lpopPushEventIdx; i++ {
        e := lpopPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < drainPushEventIdx; i++ {
        e := drainPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < stealPushEventIdx; i++ {
        e := stealPushEvents[i]
        print("Goroutine ", e.GoRoutineID, " is no longer running on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }
    for i := uint64(0); i < executePushEventIdx; i++ {
        e := executePushEvents[i]
        print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    }

    print("=== End Dump ===\n")
}
