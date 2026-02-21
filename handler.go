package main

import (
	"fmt"
	"sync"
)

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	fmt.Println(args)
	return Value{typ: "string", str: args[0].bulk}
}

var SETs = map[string]string{}
var SETmu = sync.RWMutex{}

func set(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'set' command"}
	}

	key := args[0].bulk
	val := args[1].bulk

	SETmu.Lock()
	SETs[key] = val
	SETmu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {

	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'get' command"}
	}

	key := args[0].bulk

	SETmu.RLock()
	value, ok := SETs[key]
	SETmu.RUnlock()
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

var HSETs = map[string]map[string]string{}
var HSETmu = sync.RWMutex{}

func hset(args []Value) Value {

	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	val := args[2].bulk

	HSETmu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = val
	HSETmu.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {

	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETmu.RLock()
	value, ok := HSETs[hash][key]
	HSETmu.RUnlock()
	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {

	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	HSETmu.RLock()
	defer HSETmu.RUnlock()

	m, ok := HSETs[hash]
	if !ok {
		return Value{typ: "null"}
	}

	res := make([]Value, 0, len(m)*2)
	for k, v := range m {
		res = append(res, Value{typ: "bulk", bulk: k}, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: res}
}
