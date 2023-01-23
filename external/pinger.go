package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

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
	pingsSent               = "nginx.heartbeat.ping.sent"
	pongsRecv               = "nginx.heartbeat.pong.recv"

	// Config Keys
	pluginName = "plugin-name"
	limit      = "limit"
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

func getKeyUint64(key string) uint64 {
	if key == "" {
		return 0
	}

	val := pdk.GetVar(key)
	if val == nil {
		return 0
	}

	intVal := binary.LittleEndian.Uint64(val)
	return intVal
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

	if string(topic) == pong {
		incrNumberKey(pongsRecv)
		if err != nil {
			logString(pdk.LogDebug, "process_ guest: failed incrementing pongs")
		}

		limit, ok := pdk.GetConfig(limit)
		if !ok {
			logString(pdk.LogDebug, "process_ guest: cannot determine limit, stopping")
			return 0
		}

		n, err := strconv.ParseUint(limit, 10, 64)
		if err != nil {
			// call on_error?
			logString(pdk.LogDebug, "process_ guest: cannot parse limit config, stopping")
			return 0
		}

		if getKeyUint64(pongsRecv) == n {
			logString(pdk.LogDebug, "process_ geust: limit reached")
			return 0
		}
	}

	ping := pdk.AllocateString(ping)
	defer ping.Free()
	payload := pdk.AllocateString(name)
	defer payload.Free()
	ret := process__(
		ping.Offset(),
		ping.Length(),
		payload.Offset(),
		payload.Length(),
	)
	if ret == -1 {
		setError(fmt.Errorf("process_ guest: host side process__ failed - rc: %v", ret))
		return -1
	}
	err = incrNumberKey(pingsSent)
	if err != nil {
		logString(pdk.LogDebug, "process_ guest: failed incrementing pings")
	}
	logString(pdk.LogDebug, "process_ guest: host side process__ success")

	b := buildReturn(name, topic, false, pingsSent, pongsRecv)
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
