/**
 * Copyright (c) F5, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

package core

/*
#cgo LDFLAGS: -L/usr/local/lib -lextism
//#include <stdlib.h>
#include <extism.h>
EXTISM_GO_FUNCTION(process__);
EXTISM_GO_FUNCTION(on_error__);
*/
import "C"

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"runtime"
	"runtime/cgo"
	"unsafe"

	"github.com/nginx/agent/v2/src/core/config"

	"github.com/extism/extism"
	log "github.com/sirupsen/logrus"
)

type (
	Plugin interface {
		Init(MessagePipeInterface)
		Close()
		Process(*Message)
		Info() *Info
		Subscriptions() []string
	}

	argPosition int

	processOutput struct {
		Pongs  uint64 `json:"pongs"`
		Pings  uint64 `json:"pings"`
		Plugin string `json:"plugin"`
		Topic  string `json:"topic"`
		Async  bool   `json:"async"`
	}

	/*
		metrics struct {
			InitCalls uint64
			SubsCalls uint64
			CloseCalls uint64
			InfoCalls uint64
			ProcessCalls uint64
			Plugin string
		}
	*/
)

// general constants (not iota based)
const (
	hostError = 0xFFFFFFFF
)

const (
	//process__ signature
	process__TopicOffset argPosition = iota
	process__TopicLength
	process__DataOffset
	process__DataLength
)

const (
	//on_error__ signature
	on_error__MsgOffset argPosition = iota
	on_error__MsgLength
	on_error__Code
)

var (
	symbTab []string = []string{
		"init_",
		"subscriptions_",
		"close_",
		"info_",
		"process_",
		//"metrics_", // Call to get at Vars set by guest
	}

	events map[string]struct{} = map[string]struct{}{
		AgentStarted: struct{}{},
	}

	// process__ signature
	process__TopicOffsetType = extism.I64
	process__TopicLengthType = extism.I64
	process__DataOffsetType  = extism.I64
	process__DataLengthType  = extism.I64

	process__ReturnType = extism.I32

	process__Parameters []extism.ValType = []extism.ValType{
		process__TopicOffsetType,
		process__TopicLengthType,
		process__DataOffsetType,
		process__DataLengthType,
	}

	process__ReturnTypes []extism.ValType = []extism.ValType{
		process__ReturnType,
	}

	// on_error__ signature
	on_error__MsgOffsetType = extism.I64
	on_error__MsgLengthType = extism.I64
	on_error__CodeType      = extism.I32

	on_error__ReturnType = extism.I32

	on_error__Parameters []extism.ValType = []extism.ValType{
		on_error__MsgOffsetType,
		on_error__MsgLengthType,
		on_error__CodeType,
	}

	on_error__ReturnTypes []extism.ValType = []extism.ValType{
		on_error__ReturnType,
	}
)

type ExternalPlugin struct {
	plugin          config.ExternalPlugin
	logConfig       config.LogConfig
	ctx             extism.Context
	handle          *extism.Plugin // this is a pointer to help with free logic
	functions       []extism.Function
	messagePipeline MessagePipeInterface
}

func pluginManifest(config config.ExternalPlugin) extism.Manifest {
	return extism.Manifest{
		Wasm: []extism.Wasm{
			extism.WasmFile{
				Path: config.Source.Path,
			},
		},
		Config: config.Config,
	}
}

func NewExternalPlugin(logConf config.LogConfig, config config.ExternalPlugin) (*ExternalPlugin, error) {
	e := &ExternalPlugin{
		plugin:    config,
		logConfig: logConf,
	}
	runtime.SetFinalizer(e, free)

	// abstract away an interface for waPC / wasmtime-go but not yet
	e.ctx = extism.NewContext()

	e.functions = append(
		e.functions,
		extism.NewFunction(
			"process__",
			process__Parameters,  //inputs
			process__ReturnTypes, //outputs
			C.process__,          // function pointer
			e,                    // plugin itself as userdata to function when called by WASM
		),
		extism.NewFunction(
			"on_error__",
			on_error__Parameters,
			on_error__ReturnTypes,
			C.on_error__,
			e,
		),
	)

	var err error
	handle, err := e.ctx.PluginFromManifest(
		pluginManifest(config),
		e.functions,
		true,
	)
	if err != nil {
		return nil, err
	}
	e.handle = &handle

	// validate
	for i := range symbTab {
		valid := e.handle.FunctionExists(symbTab[i])
		if !valid {
			err = fmt.Errorf("plugin does not provide symbol - %s", symbTab[i])
			return nil, err
		}

	}

	return e, err
}

func free(e *ExternalPlugin) {
	for i := range e.functions {
		e.functions[i].Free()
	}
	if e.handle != nil {
		e.handle.Free()
	}
	e.ctx.Free()
}

type publisher interface {
	Name() string
	Process__(*Message)
}

func (e *ExternalPlugin) Name() string {
	return e.plugin.Source.Name
}

func (e *ExternalPlugin) Process__(m *Message) {
	e.messagePipeline.Process(m)
	return
}

func getUint32(input *C.ExtismVal) uint32 {
	if input == nil {
		return 0
	}

	if len(input.v) != 4 {
		return 0
	}

	return ((uint32(input.v[0])) |
		(uint32(input.v[1] << 8)) |
		(uint32(input.v[2] << 16)) |
		(uint32(input.v[3] << 24)))
}

func getUint64(input *C.ExtismVal) uint64 {
	if input == nil {
		return 0
	}

	if len(input.v) != 8 {
		return 0
	}

	return ((uint64(input.v[0])) |
		(uint64(input.v[1] << 8)) |
		(uint64(input.v[2] << 16)) |
		(uint64(input.v[3] << 24)) |
		(uint64(input.v[4] << 32)) |
		(uint64(input.v[5] << 40)) |
		(uint64(input.v[6] << 48)) |
		(uint64(input.v[7] << 56)))
}

func checkMemOffset(plugin *C.ExtismCurrentPlugin, offset uint64) bool {
	if plugin == nil {
		return false
	}
	if C.extism_current_plugin_memory_length(plugin, C.uint64_t(offset)) == 0 {
		return false
	}
	return true
}

// host side asynchronous event used as `on_error__`, i.e., the guest code imports this function and invokes it
// when its `process_` has computed an answer asynchronously and the guest can't use the host side
// `process__` for an informative event
// the guest code must import it from the env when using tinygo
//     //msg_offset, msg_length, code | ret_code
//     //go:wasm-module env
//     //export on_error__
//     func on_error__(uint64, uint64, uint32) int32
//export on_error__
func on_error__(
	plugin *C.ExtismCurrentPlugin,
	inputs *C.ExtismVal, nInputs C.ExtismSize,
	outputs *C.ExtismVal, nOutputs C.ExtismSize,
	userData uintptr,
) {
	inputSlice := unsafe.Slice(inputs, nInputs)
	outputSlice := unsafe.Slice(outputs, nOutputs)
	v := cgo.Handle(userData)
	p, ok := v.Value().(publisher)
	if !ok {
		log.WithFields(
			log.Fields{
				"plugin": "unknown",
				"error":  "on_error__ called from guest with invalid userData (not a publisher)",
			},
		).Warn("on_error__ failed")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}
	log.WithFields(
		log.Fields{
			"plugin": p.Name(),
		},
	).Debug("on_error__ call from guest")

	memPtr := C.extism_current_plugin_memory(plugin)
	if memPtr == nil {
		log.WithFields(
			log.Fields{
				"plugin": p.Name(),
				"error":  "on_error__ received nil memory",
			},
		).Warn("on_error__ cannot publish")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}

	// valid memory pointer, do simple bounds checking
	mOffset := getUint64(&(inputSlice[on_error__MsgOffset]))
	mLength := getUint64(&(inputSlice[on_error__MsgLength]))

	if !checkMemOffset(plugin, mOffset) {
		log.WithFields(
			log.Fields{
				"plugin": p.Name(),
				"error":  "on_error__ inputs: msg out of bounds - unaligned with allocated block",
			},
		).Warn("on_error__ memory exception")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}
	msgPtr := unsafe.Add(unsafe.Pointer(memPtr), mOffset)
	msg := string(C.GoBytes(msgPtr, C.int(mLength)))
	log.WithFields(
		log.Fields{
			"plugin":  p.Name(),
			"message": msg,
		},
	).Debug("on_error__ publishing topic")

	// unnecessary but for explicitness right now
	binary.LittleEndian.PutUint32(outputSlice[0].v[:], 0)
}

// the host side function to be used as `process__`, i.e., the guest code imports this function and invokes it
// as process__
// the guest code must import it from the env when using tinygo
//     //signature: topic_offset, topic_length, data_offset, data_length | ret_code
//     //go:wasm-module env
//     //export process__
//     func process__(uint64, uint64, uint64, uint64) int32
//export process__
func process__(
	plugin *C.ExtismCurrentPlugin,
	inputs *C.ExtismVal, nInputs C.ExtismSize,
	outputs *C.ExtismVal, nOutputs C.ExtismSize,
	userData uintptr,
) {
	// process is the host side call
	// a plugin calls this host function which proxies a call into
	// messagePipeline.Process()
	// do this to account for the signature impedance
	inputSlice := unsafe.Slice(inputs, nInputs)
	outputSlice := unsafe.Slice(outputs, nOutputs)
	v := cgo.Handle(userData)
	p, ok := v.Value().(publisher)
	if !ok {
		log.WithFields(
			log.Fields{
				"plugin": "unknown",
				"error":  "process__ called from guest with invalid userData (not a publisher)",
			},
		).Warn("process__ failed")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}
	log.WithFields(
		log.Fields{
			"plugin": p.Name(),
		},
	).Debug("process__ call from guest")

	// verify input types
	for i := range inputSlice {
		if inputSlice[i].t != (C.ExtismValType)(process__Parameters[i]) {
			log.WithFields(
				log.Fields{
					"plugin":        p.Name(),
					"arg_position":  i,
					"type":          inputSlice[i].t,
					"expected_type": process__Parameters[i],
				},
			).Warn("process__ received invalid inputs")

			binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
			return
		}
	}

	memPtr := C.extism_current_plugin_memory(plugin)
	if memPtr == nil {
		log.WithFields(
			log.Fields{
				"plugin": p.Name(),
				"error":  "process__ received nil memory",
			},
		).Warn("process__ cannot publish")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}

	// valid memory pointer, do simple bounds checking
	tOffset := getUint64(&(inputSlice[process__TopicOffset]))
	tLength := getUint64(&(inputSlice[process__TopicLength]))
	dOffset := getUint64(&(inputSlice[process__DataOffset]))
	dLength := getUint64(&(inputSlice[process__DataLength]))

	if !checkMemOffset(plugin, tOffset) {
		log.WithFields(
			log.Fields{
				"plugin": p.Name(),
				"error":  "process__ inputs: topic out of bounds - unaligned with allocated block",
			},
		).Warn("process__ memory exception")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}
	if !checkMemOffset(plugin, dOffset) {
		log.WithFields(
			log.Fields{
				"plugin": p.Name(),
				"error":  "process__ inputs: data out of bounds - unaligned with allocated block",
			},
		).Warn("process__ memory exception")
		binary.LittleEndian.PutUint32(outputSlice[0].v[:], uint32(hostError))
		return
	}

	topicPtr := unsafe.Add(unsafe.Pointer(memPtr), tOffset)
	topic := string(C.GoBytes(topicPtr, C.int(tLength)))

	dataPtr := unsafe.Add(unsafe.Pointer(memPtr), dOffset)
	data := string(C.GoBytes(dataPtr, C.int(dLength)))

	log.WithFields(
		log.Fields{
			"plugin": p.Name(),
			"topic":  topic,
		},
	).Debug("process__ publishing topic")

	p.Process__(NewMessage(topic, data))

	// unnecessary but for explicitness right now
	binary.LittleEndian.PutUint32(outputSlice[0].v[:], 0)
}

func (e *ExternalPlugin) Init(pipeline MessagePipeInterface) {
	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("calling init_")

	e.messagePipeline = pipeline

	_, err := e.handle.Call("init_", nil)
	if err != nil {
		panic(fmt.Sprintf("%s failed calling init_: %s", e.Name(), err.Error()))
	}

	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("init_ return")
}

func (e *ExternalPlugin) Close() {
	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("calling close_")

	_, err := e.handle.Call("close_", nil)
	if err != nil {
		log.WithFields(
			log.Fields{
				"plugin": e.Name(),
				"error":  err,
			},
		).Warn("close_ error")
	}
}

func (e *ExternalPlugin) Process(msg *Message) {
	log.WithFields(
		log.Fields{
			"plugin":  e.Name(),
			"message": msg,
		},
	).Debug("Process called")

	// only support string message data and a subset of topics for external plugins
	maybePayload, isString := msg.Data().(string)
	_, isSupported := events[msg.Topic()]
	if !isString && !isSupported {
		log.WithFields(
			log.Fields{
				"plugin":               e.Name(),
				"topic":                msg.Topic(),
				"error":                "external plugin topic unimplemented",
				"is_string_data":       isString,
				"is_unsupported_topic": !isSupported,
			},
		).Warn("external plugin process error")
		return
	}

	var msgBytes []byte
	var tmp bytes.Buffer
	tmp.Write([]byte(`{"topic":"`))
	tmp.Write([]byte(msg.Topic()))
	tmp.Write([]byte(`","data":"`))
	if isString {
		tmp.Write([]byte(maybePayload))
		tmp.Write([]byte(`"}`))
	} else {
		tmp.Write([]byte(`unimplemented interface"}`))
	}

	// TODO, if guest process_ does something asynchronous it can call back
	// into any host function, is that necessary? error reporting is important, but not sure
	// about a result, any result should be communicated via `process__` and a message?
	//
	// 1) is there a Promise style pattern here?
	// 2) does the host need to know there is an asynchronous operation?
	// 3) what events are required on the host?
	//   result__(offset, length, code)
	//   error__(offset, length)
	msgBytes = tmp.Bytes()
	out, err := e.handle.Call("process_", msgBytes)
	if err != nil {
		log.WithFields(
			log.Fields{
				"plugin": e.Name(),
				"topic":  msg.Topic(),
				"error":  err,
			},
		).Warn("process_ invocation error")
		return
	}

	// TODO this could be quicker using fastjson
	o := processOutput{}
	err = json.Unmarshal(out, &o)
	if err != nil {
		log.WithFields(
			log.Fields{
				"plugin": e.Name(),
				"topic":  msg.Topic(),
				"error":  err,
			},
		).Warn("process_ failed to unmarshal output")
		return
	}

	log.WithFields(
		log.Fields{
			"plugin":     e.Name(),
			"topic":      msg.Topic(),
			"out.plugin": o.Plugin,
			"out.topic":  o.Topic,
			"out.pings":  o.Pongs,
			"out.pongs":  o.Pings,
			"out.async":  o.Async,
		},
	).Debug("process_ returned")

	return
}

func (e *ExternalPlugin) Info() *Info {
	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("calling info_")

	out, err := e.handle.Call("info_", nil)
	if err != nil {
		log.WithFields(
			log.Fields{
				"plugin":      e.Name(),
				"call_output": string(out),
				"error":       err,
			},
		).Warn("info_ failed")
		return nil
	}

	var info *Info
	if len(out) != 0 {
		tmp := struct {
			Name    string
			Version string
		}{}
		err := json.Unmarshal(out, &tmp)
		if err != nil {
			log.WithFields(
				log.Fields{
					"plugin":      e.Name(),
					"call_output": string(out),
					"error":       err,
				},
			).Warn("cannot parse info_ output")
		}
		info = NewInfo(tmp.Name, tmp.Version)
	}
	return info
}

func (e *ExternalPlugin) Subscriptions() []string {
	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("calling subscriptions_")

	out, err := e.handle.Call("subscriptions_", nil)
	if err != nil {
		log.WithFields(
			log.Fields{
				"plugin":      e.Name(),
				"call_output": string(out),
				"error":       err,
			},
		).Warn("subscriptions_ failed")
		return nil
	}

	var subs []string
	if len(out) != 0 {
		err := json.Unmarshal(out, &subs)
		if err != nil {
			log.WithFields(
				log.Fields{
					"plugin":      e.Name(),
					"call_output": string(out),
					"error":       err,
				},
			).Warn("cannot parse subscriptions_ output")
		}
	}

	subs = append(subs, e.plugin.Subscriptions...)
	log.WithFields(
		log.Fields{
			"plugin":        e.Name(),
			"subscriptions": subs,
		},
	).Debug("registering subscriptions")

	return subs
}
