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

func NanotimeNow() int64{
	return runtime.Nanotime()
}

func DumpInstrumentationLogsToFile(filename string) {

	limit := atomic.LoadUint64(&runtime.GoEventIdx)

	if limit > runtime.MaxEvents {
		limit = runtime.MaxEvents
	}

	f, err := os.Create(filename)
	if err != nil {
		println("Failed to create dump file:", err.Error())
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)

	// Write each event as a single JSON object on its own line.
	for i := uint64(0); i < limit; i++ {
		event := runtime.GoEvents[i&(runtime.MaxEvents-1)]
		if err := enc.Encode(event); err != nil {
			println("Failed to encode event:", err.Error())
			return
		}
	}

}

func DumpQSizeLogsToFile(filename string) {

	limit := atomic.LoadUint64(&runtime.QSizeIdx)

	if limit > runtime.MaxEvents {
		limit = runtime.MaxEvents
	}

	f, err := os.Create(filename)
	if err != nil {
		println("Failed to create dump file:", err.Error())
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for i := uint64(0); i < limit; i++ {
		event := runtime.QueueLenTimestamps[i&(runtime.MaxEvents-1)]
		if err := enc.Encode(event); err != nil {
			println("Failed to encode event:", err.Error())
			return
		}
	}
}

func DumpGStatusLogsToFile(filename string) {

	limit := atomic.LoadUint64(&runtime.ChangeStatusIdx)

	if limit > runtime.MaxEvents {
		limit = runtime.MaxEvents
	}

	f, err := os.Create(filename)
	if err != nil {
		println("Failed to create dump file:", err.Error())
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for i := uint64(0); i < limit; i++ {
		event := runtime.ChangeStatusEvents[i&(runtime.MaxEvents-1)]
		if err := enc.Encode(event); err != nil {
			println("Failed to encode event:", err.Error())
			return
		}
	}
}
