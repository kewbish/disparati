package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("read", func(msg maelstrom.Message) error {
		ivalues, err := kv.Read(context.Background(), "map")
		var values map[string]interface{}
		if err != nil {
			values = make(map[string]interface{})
		} else {
			values = ivalues.(map[string]interface{})
		}
		var finalCounter float64 = 0
		for _, v := range values {
			finalCounter += v.(float64)
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

		ivalues, err := kv.Read(context.Background(), "map")
		var values map[string]interface{}
		if err != nil {
			values = make(map[string]interface{})
		} else {
			values = ivalues.(map[string]interface{})
		}
		if _, exists := values[msg.Src]; !exists {
			values[msg.Src] = float64(0)
		}
		ivalue, _ := values[msg.Src]
		value := ivalue.(float64)
		values[msg.Src] = float64(value) + body["delta"].(float64)
		err = kv.Write(context.Background(), "map", values)
		log.Default().Print(values, err)

		body = make(map[string]any)
		body["type"] = "add_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
