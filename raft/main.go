package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var timeout time.Duration = 500 * time.Millisecond

func main() {
	n := maelstrom.NewNode()
	var mx sync.Mutex
	mapState := make(map[any]any)

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]

		mx.Lock()
		newMapState, value, err := applyRead(mapState, key)
		mapState = newMapState
		mx.Unlock()
		if err != nil {
			return err
		}

		body = make(map[string]any)
		body["type"] = "read_ok"
		body["value"] = value

		return n.Reply(msg, body)
	})

	n.Handle("write", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]
		value := body["value"]

		mx.Lock()
		newMapState, err := applyWrite(mapState, key, value)
		mapState = newMapState
		mx.Unlock()
		if err != nil {
			return err
		}

		body = make(map[string]any)
		body["type"] = "write_ok"

		return n.Reply(msg, body)
	})

	n.Handle("cas", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]
		from := body["from"]
		to := body["to"]

		mx.Lock()
		newMapState, err := applyCAS(mapState, key, from, to)
		mapState = newMapState
		mx.Unlock()
		if err != nil {
			return err
		}

		body = make(map[string]any)
		body["type"] = "cas_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func applyRead(mapState map[any]any, key any) (newMapState map[any]any, value any, err error) {
	if v, ok := mapState[key]; ok {
		return mapState, v, nil
	} else {
		maelstromError := maelstrom.NewRPCError(404, "Key not found")
		return mapState, "", maelstromError
	}
}

func applyWrite(mapState map[any]any, key any, value any) (newMapState map[any]any, err error) {
	newMapState = make(map[any]any)
	for k, v := range mapState {
		newMapState[k] = v
	}
	newMapState[key] = value
	return newMapState, nil
}

func applyCAS(mapState map[any]any, key any, from any, to any) (newMapState map[any]any, err error) {
	if v, ok := mapState[key]; ok {
		if v != from {
			return mapState, maelstrom.NewRPCError(400, "Value is not equal to from")
		}
		newMapState = make(map[any]any)
		for k, v := range mapState {
			newMapState[k] = v
		}
		newMapState[key] = to
		return newMapState, nil
	} else {
		return mapState, maelstrom.NewRPCError(404, "Key not found")
	}
}
