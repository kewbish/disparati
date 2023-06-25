package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var mx sync.Mutex
	cache := make(map[string]int)

	n.Handle("init", func(msg maelstrom.Message) error {
		if err := kv.Write(context.Background(), n.ID(), 0); err != nil {
			log.Default().Fatal(err)
			return err
		}
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		finalCounter := 0
		for _, id := range n.NodeIDs() {
			if id == n.ID() {
				value, err := kv.ReadInt(context.Background(), n.ID())
				if err != nil {
					finalCounter += cache[n.ID()]
				} else {
					finalCounter += value
					cache[n.ID()] = value
				}

			} else {
				value, err := n.SyncRPC(context.Background(), id, map[string]any{"type": "update_from_local"})
				if err != nil {
					finalCounter += cache[n.ID()]
				} else {
					var body map[string]any
					if err := json.Unmarshal(value.Body, &body); err != nil {
						return err
					}

					value := int(body["value"].(float64))
					finalCounter += value
					cache[id] = value
				}
			}
		}

		body := make(map[string]any)
		body["type"] = "read_ok"
		body["value"] = finalCounter

		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mx.Lock()
		value, err := kv.ReadInt(context.Background(), n.ID())
		if err != nil {
			value = 0
		}
		err = kv.Write(context.Background(), n.ID(), value+int(body["delta"].(float64)))
		mx.Unlock()

		body = make(map[string]any)
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	n.Handle("update_from_local", func(msg maelstrom.Message) error {
		value, err := kv.ReadInt(context.Background(), n.ID())
		if err != nil {
			return err
		}
		return n.Reply(msg, map[string]any{"type": "update_from_local_ok", "value": value})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
