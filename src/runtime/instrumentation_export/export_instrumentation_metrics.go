package instrumentation_export

import (
	"encoding/json"
	"os"
	"runtime"
	"sync/atomic"
)

func DumpInstrumentationLogs() {
	runtime.Dump_instrumentation_logs()
}

func DumpQSizeLogs() {
	runtime.Dump_qsize_logs()
}

func DumpGStatusLogs() {
	runtime.Dump_change_status_logs()
}

func DumpInstrumentationLogsToFile(filename string) {

	limit := atomic.LoadUint64(&runtime.GoEventIdx)

	if limit > runtime.MaxEvents {
		limit = runtime.MaxEvents
	}

	// in case we do not meet max we must fit the data to the appropriate size
	// array
	events := make([]runtime.SchedEvent, limit)
	for i := uint64(0); i < limit; i++ {
		events[i] = runtime.GoEvents[i&(runtime.MaxEvents-1)]
	}

	f, err := os.Create(filename)
	if err != nil {
		println("Failed to create dump file:", err.Error())
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	enc.Encode(events)
}
