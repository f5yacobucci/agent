package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	pdk "github.com/extism/go-pdk"
	"github.com/valyala/fastjson"
)

/*
#include "runtime/extism-pdk.h"
*/
import "C"

//go:wasm-module env
//export process__
func process__(uint64, uint64, uint64, uint64) int32

//go:wasm-module env
//export on_return__
func on_return__(uint64, uint64, uint32) int32

var (
	name    = ""
	version = ""
)

const (
	// topics - should pull these from agent, but CGO whines
	agentStarted = "agent.started"
	ping         = "nginx.plugin.external.ping"
	pong         = "nginx.plugin.external.pong"

	subscriptions = `["` + agentStarted + `"]`

	// Keys
	invocationsInitTimes    = "invocations.init.times"
	invocationsSubsTimes    = "invocations.subs.times"
	invocationsCloseTimes   = "invocations.close.times"
	invocationsInfoTimes    = "invocations.info.times"
	invocationsProcessTimes = "invocations.process.times"
	pingsRecv               = "nginx.heartbeat.ping.recv"
	pongsSent               = "nginx.heartbeat.pong.sent"

	// Config Keys
	pluginName = "plugin-name"
)

// Make these an import
func setError(err error) {
	if err == nil {
		return
	}

	setErrorString(err.Error())
	return
}

func setErrorString(err string) {
	mem := pdk.AllocateString(err)
	defer mem.Free()
	C.extism_error_set(mem.Offset())
}

func incrNumberKey(key string) error {
	if key == "" {
		return nil
	}

	val := pdk.GetVar(key)
	if val == nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 1)
		pdk.SetVar(key, b)

		return nil
	}

	intVal := binary.LittleEndian.Uint64(val)
	intVal = intVal + 1
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, intVal)
	pdk.SetVar(key, b)

	return nil
}

func logString(l pdk.LogLevel, s string) {
	mem := pdk.AllocateString(s)
	defer mem.Free()
	pdk.LogMemory(l, mem)
}

func buildReturn(
	plugin string,
	topic []byte,
	async bool,
	pingKey string,
	pongKey string,
) bytes.Buffer {
	var b bytes.Buffer
	b.Write([]byte(`{"topic":"`))
	b.Write(topic)
	b.Write([]byte(`","pings":`))
	incoming := pdk.GetVar(pingKey)
	if incoming == nil {
		logString(pdk.LogDebug, "process_ guest: could not get pings recv")
		b.Write([]byte(`0,"pongs":`))
	} else {
		v := binary.LittleEndian.Uint64(incoming)
		b.Write([]byte(strconv.FormatUint(v, 10)))
		b.Write([]byte(`,"pongs":`))
	}
	outgoing := pdk.GetVar(pongKey)
	if outgoing == nil {
		logString(pdk.LogDebug, "process_ guest: could not get pongs sent")
		b.Write([]byte(`0`))
	} else {
		v := binary.LittleEndian.Uint64(outgoing)
		b.Write([]byte(strconv.FormatUint(v, 10)))
	}

	b.Write([]byte(`,"plugin":"`))
	b.Write([]byte(plugin))
	b.Write([]byte(`","async":`))
	b.Write([]byte(strconv.FormatBool(async)))
	b.Write([]byte(`}`))

	return b
}

// Plugin logic below

//export init_
func init_() int32 {
	// DEBUG Metrics
	err := incrNumberKey(invocationsInitTimes)
	if err != nil {
		setError(err)
		return -1
	}

	// XXX figure out logging, try 0.2.0 release or git HEAD
	// XXX add the plugin name to each log
	logString(pdk.LogDebug, "init_ guest: entry")

	logString(pdk.LogDebug, "init_ guest: exit")
	return 0
}

//export close_
func close_() int32 {
	// DEBUG Metrics
	err := incrNumberKey(invocationsCloseTimes)
	if err != nil {
		setError(err)
		return -1
	}

	logString(pdk.LogDebug, "close_ guest: entry")

	logString(pdk.LogDebug, "close_ guest: exit")
	return 0
}

//export process_
func process_() int32 {
	// DEBUG Metrics
	err := incrNumberKey(invocationsProcessTimes)
	if err != nil {
		setError(err)
		return -1
	}

	logString(pdk.LogDebug, "process_ guest: entry")

	name, ok := pdk.GetConfig(pluginName)
	if !ok {
		logString(pdk.LogDebug, "process_ guest: cannot get self name")
		name = "unknown"
	}

	input := pdk.Input()

	var p fastjson.Parser
	v, err := p.ParseBytes(input)
	if err != nil {
		setError(err)
		return -1
	}

	topic := v.GetStringBytes("topic")
	if topic == nil {
		setErrorString("process_ guest: cannot determine the event topic")
		return -1
	}

	if string(topic) == ping {
		incrNumberKey(pingsRecv)
		if err != nil {
			logString(pdk.LogDebug, "process_ guest: failed incrementing pings")
		}

		// bit contrived, on a ping wait in a goroutine before sending the response
		// exercised some level of asynchronous behavior
		// this will run as if GO_MAXPROCS=1, but should still give us non-blocking
		// behavior with WASM (fingers crossed)
		// uncomment ASYNC markers to test go routines - as of yet not working
		//ASYNC go func(then time.Time) {
		/*
			then := time.Now()
			//ASYNC time.Sleep(30 * time.Second)

			msgOut := fmt.Sprintf(
				"{\"plugin\":\"ponger\",\"returned\":\"yes\",\"time-delay\":%v}",
				time.Now().Sub(then),
			)

			retMsg := pdk.AllocateString(msgOut)
			defer retMsg.Free()
			ret := on_return__(retMsg.Offset(), retMsg.Length(), 0)
			if ret == -1 {
				// error from an error notification...turtles
				logString(pdk.LogDebug, fmt.Sprintf("process_ guest: async failure, host side on_return__ - rc: %v", ret))
				//ASYNC return
				return -1
			}
		*/

		then := time.Now()

		msgOut := fmt.Sprintf(`{"plugin":"%s","on_return":true,"time-delay":%v}`,
			name,
			time.Now().Sub(then),
		)
		retMsg := pdk.AllocateString(msgOut)
		defer retMsg.Free()
		ret := on_return__(retMsg.Offset(), retMsg.Length(), 0)
		if ret == -1 {
			logString(pdk.LogDebug, fmt.Sprintf("process_ guest: async failure, host side on_return__ - rc: %v", ret))
			return -1
		}

		pong := pdk.AllocateString(pong)
		defer pong.Free()
		payload := pdk.AllocateString(name)
		defer payload.Free()
		ret = process__(
			pong.Offset(),
			pong.Length(),
			payload.Offset(),
			payload.Length(),
		)
		if ret == -1 {
			logString(pdk.LogDebug, fmt.Sprintf("process_ guest: host side process__ failed - rc: %v", ret))
			//ASYNC return
			return -1
		}
		err = incrNumberKey(pongsSent)
		if err != nil {
			logString(pdk.LogDebug, "process_ guest: failed incrementing pongs")
		}
		logString(pdk.LogDebug, "process_ guest: host side process__ success")
		//ASYNC }(time.Now())
	}

	b := buildReturn(name, topic, true, pingsRecv, pongsSent)
	mem := pdk.AllocateBytes(b.Bytes())
	defer mem.Free()
	pdk.OutputMemory(mem)

	logString(pdk.LogDebug, "process_ guest: exit")
	return 0
}

//export info_
func info_() int32 {
	// DEBUG Metrics
	err := incrNumberKey(invocationsInfoTimes)
	if err != nil {
		setError(err)
		return -1
	}

	logString(pdk.LogDebug, "info_ guest: entry")

	arena := &fastjson.Arena{}
	json := arena.NewObject()
	json.Set("name", arena.NewString(name))
	json.Set("version", arena.NewString(version))

	var b []byte
	enc := json.MarshalTo(b)

	mem := pdk.AllocateBytes(enc)
	defer mem.Free()
	pdk.OutputMemory(mem)

	logString(pdk.LogDebug, "info_ guest: exit")
	return 0
}

//export subscriptions_
func subscriptions_() int32 {
	// DEBUG Metrics
	err := incrNumberKey(invocationsSubsTimes)
	if err != nil {
		setError(err)
		return -1
	}

	logString(pdk.LogDebug, "subscriptions_ guest: entry")

	subs := pdk.AllocateString(subscriptions)
	defer subs.Free()
	pdk.OutputMemory(subs)

	logString(pdk.LogDebug, "subscriptions_ guest: exit")
	return 0
}

// https://github.com/tinygo-org/tinygo/issues/2703
func main() {}
