package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	broadcastValues []interface{}
	topology        map[string]interface{}
	valMutex        sync.Mutex
	topMutex        sync.Mutex
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valMutex.Lock()
		broadcastValues = append(broadcastValues, body["message"])
		valMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "broadcast_ok"
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "read_ok"
		valMutex.Lock()
		body["messages"] = broadcastValues
		valMutex.Unlock()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		topMutex.Lock()
		topology = body["topology"].(map[string]interface{})
		topMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
