package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	values := make(map[interface{}]bool)
	var valueMutex sync.RWMutex

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

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
