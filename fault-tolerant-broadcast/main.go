package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	received    map[interface{}]bool
	neighbours  []interface{}
	toBroadcast map[string]map[interface{}]bool
	valMutex    sync.Mutex
	topMutex    sync.Mutex
)

func main() {
	n := maelstrom.NewNode()
	received = make(map[interface{}]bool)
	toBroadcast = make(map[string][]interface{})

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			tick := time.Tick(500 * time.Millisecond)
			for range tick {
				keys := make([]interface{}, 0, len(values))
				for k := range values {
					keys = append(keys, k)
				}

				for _, neighbour := range neighbours {
					if _, exists := toBroadcast; exists && neighbour != n.ID() {
						for body := range toBroadcast[neighbour] {
							err := n.RPC(neighbour, body, func(msg maelstrom.Message) error {
								return nil
							})
							if err == nil {
							}
						}
					}
				}
			}
		}()

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		valMutex.Lock()
		received[body["message"]] = true

		for _, neighbour := range neighbours {
			if neighbour == msg.Src {
				continue
			}
			err := n.RPC(neighbour.(string), body, func(msg maelstrom.Message) error {
				return nil
			})
			if err != nil {
				if _, exists := toBroadcast[neighbour.(string)]; !exists {
					toBroadcast[neighbour.(string)] = make(map[interface{}]bool)
				}
				toBroadcast[neighbour.(string)][body] = true
			}
		}
		valMutex.Unlock()

		if _, exists := body["msg_id"]; exists {
			body = make(map[string]any)
			body["type"] = "broadcast_ok"
			return n.Reply(msg, body)
		}

		return nil
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
		neighbours = ((body["topology"].(map[string]interface{}))[n.ID()]).([]interface{})
		topMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
