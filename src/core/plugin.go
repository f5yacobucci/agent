/**
 * Copyright (c) F5, Inc.
 *
 * This source code is licensed under the Apache License, Version 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

package core

import "C"
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"

	"github.com/nginx/agent/v2/src/core/config"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	log "github.com/sirupsen/logrus"
	wapcsdk "github.com/wapc/wapc-go"
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

var (
	events map[string]struct{} = map[string]struct{}{
		AgentStarted: struct{}{},
	}
)

type ExternalPlugin struct {
	plugin          config.ExternalPlugin
	ctx             context.Context
	handle          wapcsdk.Instance
	messagePipeline MessagePipeInterface
}

func NewExternalPlugin(ctx context.Context, config config.ExternalPlugin, router *Router, module wapcsdk.Module) (*ExternalPlugin, error) {
	e := &ExternalPlugin{
		plugin: config,
		ctx:    ctx,
	}

	var err error
	e.handle, err = module.Instantiate(ctx) /* can you cancel here? ctx.WithCancel() /shrug */
	if err != nil {
		return nil, fmt.Errorf("external plugin cannot wrap wapc instance - %w", err)
	}
	runtime.SetFinalizer(e, free)

	router.RegisterBinding(e.Name(), "messagebus", "process__", e.Process__)
	return e, err
}

func free(e *ExternalPlugin) {
	e.handle.Close(e.ctx)
}

func (e *ExternalPlugin) Name() string {
	return e.plugin.Source.Name
}

func (e *ExternalPlugin) Process__(payload []byte) ([]byte, error) {
	m := struct {
		Topic string
		Data  string
	}{}
	err := json.Unmarshal(payload, &m)
	if err != nil {
		return nil, fmt.Errorf("payload cannot be unmarshaled to topic and data: %w", err)
	}

	e.messagePipeline.Process(NewMessage(m.Topic, m.Data))
	return nil, nil
}

func (e *ExternalPlugin) Init(pipeline MessagePipeInterface) {
	log.WithFields(
		log.Fields{
			"plugin": e.Name(),
		},
	).Debug("calling init_")

	e.messagePipeline = pipeline

	//TODO, use Apex, better serial/deserial operations from an IDL
	//it's easier to do this than pull out wasiconfig and add environmental
	//keys at this point
	input, err := json.Marshal(e.plugin.Config)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error":  err.Error(),
				"plugin": e.Name(),
			},
		).Warn("cannot set init_ input, plugin functions may be effected")
	}
	_, err = e.handle.Invoke(e.ctx, "init_", input)
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

	_, err := e.handle.Invoke(e.ctx, "close_", nil)
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

	msgBytes = tmp.Bytes()
	out, err := e.handle.Invoke(e.ctx, "process_", msgBytes)
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

	out, err := e.handle.Invoke(e.ctx, "info_", nil)
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

	out, err := e.handle.Invoke(e.ctx, "subscriptions_", nil)
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

type (
	Callback func([]byte) ([]byte, error)

	Router struct {
		lock sync.Mutex
		tree *iradix.Tree[Callback]
	}
)

func NewRouter() *Router {
	return &Router{
		lock: sync.Mutex{},
		tree: iradix.New[Callback](),
	}
}

func (r *Router) createKey(binding, namespace, operation string) []byte {
	var b bytes.Buffer
	b.Write([]byte(namespace))
	b.Write([]byte(":"))
	b.Write([]byte(operation))
	if binding != "" {
		b.Write([]byte(":"))
		b.Write([]byte(binding))
	}

	return b.Bytes()
}

func (r *Router) Route(_ context.Context, binding, namespace, operation string, payload []byte) ([]byte, error) {
	key := r.createKey(binding, namespace, operation)

	r.lock.Lock()
	cb, found := r.tree.Get(key)
	r.lock.Unlock()
	if !found {
		return nil, fmt.Errorf("no registered host call for - %v::%v::%v", namespace, operation, binding)
	}
	log.WithFields(
		log.Fields{
			"binding":      binding,
			"namespace":    namespace,
			"operation":    operation,
			"key":          string(key),
			"registration": found,
		},
	).Debug("binding registration found, calling host function")

	return cb(payload)
}

func (r *Router) RegisterBinding(binding, namespace, operation string, cb Callback) (bool, error) {
	if namespace == "" || operation == "" {
		return false, fmt.Errorf("namespace and operation are required - %v::%v::%v", namespace, operation, binding)
	}

	key := r.createKey(binding, namespace, operation)

	r.lock.Lock()
	defer r.lock.Unlock()
	t, cb, set := r.tree.Insert(key, cb)
	r.tree = t
	if !set {
		return false, fmt.Errorf("could not register binding - %v::%v::%v", namespace, operation, binding)
	}
	if cb != nil {
		return true, nil
	}
	return false, nil
}
