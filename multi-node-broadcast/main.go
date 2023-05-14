package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	received    map[interface{}]bool
	broadcasted map[interface{}]bool
	topology    map[string]interface{}
	valMutex    sync.Mutex
	topMutex    sync.Mutex
)

func main() {
	// https://pages.cs.wisc.edu/~tvrdik/13/html/Section13.html
	n := maelstrom.NewNode()
	broadcasted = make(map[interface{}]bool)
	received = make(map[interface{}]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valMutex.Lock()
		received[body["message"]] = true
		valMutex.Unlock()

		_, hasBroadcasted := broadcasted[body["message"]]
		if topology != nil && !hasBroadcasted {
			neighbours, _ := topology[n.ID()].([]interface{})
			for _, neighbour := range neighbours {
				n.Send(neighbour.(string), body)
			}
		}
		broadcasted[body["message"]] = true

		body = make(map[string]any)
		body["type"] = "broadcast_ok"

		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "read_ok"
		valMutex.Lock()
		var keys []interface{}
		for k := range received {
			keys = append(keys, k)
		}
		body["messages"] = keys
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

		neighbours, _ := topology[n.ID()].([]interface{})
		for k := range received {
			_, hasBroadcast := broadcasted[k]
			if hasBroadcast {
				continue
			}
			for _, neighbour := range neighbours {
				n.Send(neighbour.(string), k)
			}
			broadcasted[k] = true
		}

		body = make(map[string]any)
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
