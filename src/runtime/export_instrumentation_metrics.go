package runtime

import "runtime/internal/atomic"

func DumpCreationLogs() {
    dump_instrumentation_logs()
}

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
    GOROUTINE_READY
    GOROUTINE_IDLE
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
    GOROUTINE_READY: "Goroutine set to Ready",
    GOROUTINE_IDLE: "Goroutine set to Idle",
}

type schedEvent struct {
    ActionID        int
    Timestamp   	int64  // timestamp (nanoseconds)
    GoRoutineID  	int64  // goroutine ID
    ProcessorID     int32  // processor ID
    WaitReason      uint8 // (waitReason) Reason why the gorouine was put to wait if relevant action, 66 is a no-op (invalid)
}

var (
    goEvents [maxEvents]schedEvent
    goEventIdx uint64
    // goCreations [maxEvents]schedEvent
    // goCreationIdx uint64
    // tailPushEvents [maxEvents]schedEvent
    // tailPushEventIdx uint64
    // headPushEvents [maxEvents]schedEvent
    // headPushEventIdx uint64
    // globalPushEvents [maxEvents]schedEvent
    // globalPushEventIdx uint64
    // g2lPushEvents [maxEvents]schedEvent
    // g2lPushEventIdx uint64
    // lpopPushEvents [maxEvents]schedEvent
    // lpopPushEventIdx uint64
    // drainPushEvents [maxEvents]schedEvent
    // drainPushEventIdx uint64
    // stealPushEvents [maxEvents]schedEvent
    // stealPushEventIdx uint64
    // executePushEvents [maxEvents]schedEvent
    // executePushEventIdx uint64
)

func log_goroutine_creation(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(GOROUTINE_CREATION, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_tail_push(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(LOCAL_QUEUE_TAIL, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_head_push(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(LOCAL_QUEUE_HEAD, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_global_push(gp *g){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        log_entry := &goEvents[idx]
        log_event(GLOBAL_QUEUE_PUSH, gp, pid, log_entry, 66);
    }
}

func log_goroutine_global_to_local(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(GLOBAL_TO_LOCAL, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_pop(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(LOCAL_QUEUE_POP, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_drain(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(LOCAL_QUEUE_DRAIN, gp, pid, log_entry, 66);
    }
}

func log_goroutine_local_steal(gp *g, pp *p){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if pp != nil {
            pid = pp.id
        }
        log_entry := &goEvents[idx]
        log_event(PROCESSOR_WORK_STEAL, gp, pid, log_entry, 66);
    }
}

func log_goroutine_execution(gp *g){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        log_entry := &goEvents[idx]
        log_event(GOROUTINE_EXECUTION, gp, pid, log_entry, 66);
    }
}

func log_goroutine_ready(gp *g){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        log_entry := &goEvents[idx]
        log_event(GOROUTINE_READY, gp, pid, log_entry, 66);
    }
}

func log_goroutine_idle(gp *g, wr waitReason){
	idx := atomic.Xadd64(&goEventIdx, 1) - 1
    if idx < maxEvents {
        pid := int32(-1)
        if gp.m != nil {
            pid = int32(gp.m.id)
        }
        log_entry := &goEvents[idx]
        log_event(GOROUTINE_IDLE, gp, pid, log_entry, uint8(wr))
    }
}

func log_event(actionID int, gp *g, pid int32, log_entry *schedEvent, waitReason uint8){
    log_entry.Timestamp = nanotime()
    log_entry.GoRoutineID = int64(gp.goid)
    log_entry.ActionID = actionID
    log_entry.ProcessorID = pid
    log_entry.WaitReason = waitReason
}

func dump_instrumentation_logs() {
    if !instrumentationEnabled {
        print("Instrumentation disabled\n")
        return
    }

    print("=== Instrumentation Dump ===\n")
    // for i := uint64(0); i < goCreationIdx; i++ {
    //     e := goCreations[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < tailPushEventIdx; i++ {
    //     e := tailPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < headPushEventIdx; i++ {
    //     e := headPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < globalPushEventIdx; i++ {
    //     e := globalPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < g2lPushEventIdx; i++ {
    //     e := g2lPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < lpopPushEventIdx; i++ {
    //     e := lpopPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < drainPushEventIdx; i++ {
    //     e := drainPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < stealPushEventIdx; i++ {
    //     e := stealPushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " is no longer running on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    // for i := uint64(0); i < executePushEventIdx; i++ {
    //     e := executePushEvents[i]
    //     print("Goroutine ", e.GoRoutineID, " ran on P", e.ProcessorID, " at ", e.Timestamp, " action: ", ActionIDStrings[e.ActionID], "\n")
    // }
    for i := uint64(0); i < goEventIdx; i++ {
        e := goEvents[i]
        print("Time: ", e.Timestamp, " - Goroutine ", e.GoRoutineID, " action: ", ActionIDStrings[e.ActionID])
        // " ran on P", e.ProcessorID,
        if(e.ActionID == PROCESSOR_WORK_STEAL){
            print(", Stolen from Processor P", e.ProcessorID)
        }
        if(e.ActionID == GOROUTINE_IDLE){
            print(", Reason: ", waitReasonStrings[waitReason(e.WaitReason)])
        }
        if (!(e.ActionID == GOROUTINE_EXECUTION) && !(e.ActionID == GOROUTINE_READY) && !(e.ActionID == PROCESSOR_WORK_STEAL) && !(e.ActionID == GLOBAL_QUEUE_PUSH)){
            print(", ran on P", e.ProcessorID,)
        }
        print("\n")
    }

    print("=== End Dump ===\n")
}
