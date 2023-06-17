package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	values := make(map[interface{}]bool)
	var valueMutex sync.RWMutex

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			tick := time.Tick(500 * time.Millisecond)
			for range tick {
				valueMutex.RLock()
				keys := make([]interface{}, 0, len(values))
				for k := range values {
					keys = append(keys, k)
				}
				valueMutex.RUnlock()

				for _, neighbour := range n.NodeIDs() {
					if neighbour != n.ID() {
						n.RPC(neighbour, map[string]any{"type": "replicate", "set": keys}, func(msg maelstrom.Message) error {
							return nil
						})
					}
				}
			}
		}()

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		valueMutex.RLock()
		keys := make([]interface{}, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		valueMutex.RUnlock()

		body := make(map[string]any)
		body["type"] = "read_ok"
		body["value"] = keys

		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valueMutex.Lock()
		values[body["element"]] = true
		valueMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valueMutex.Lock()
		for k := range (body["set"]).([]interface{}) {
			values[k] = true
		}
		valueMutex.Unlock()

		finalBody := make(map[string]any)
		finalBody["type"] = "replicate_ok"

		return n.Reply(msg, finalBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
