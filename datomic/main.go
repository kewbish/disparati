package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		transaction := body["txn"].([]interface{})
		new_transaction := make([][]interface{}, 0)
		current_store_json, err := kv.Read(context.Background(), "root")
		var current_store map[string][]interface{}
		if err != nil || current_store_json == nil {
			current_store = make(map[string][]interface{})
		} else {
			if err := json.Unmarshal([]byte(current_store_json.(string)), &current_store); err != nil {
				current_store = make(map[string][]interface{})
			}
		}

		for _, mop := range transaction {
			mop := mop
			mop_interface := mop.([]interface{})
			action, key, value := mop_interface[0], mop_interface[1], mop_interface[2]
			skey := fmt.Sprintf("%d", int64(key.(float64)))

			current_value, exists := current_store[skey]
			if !exists {
				current_value = make([]interface{}, 0)
			}
			new_mop := make([]interface{}, 3)
			new_mop[0] = action
			new_mop[1] = key
			switch action {
			case "r":
				new_mop[2] = current_value
			case "append":
				fallthrough
			default:
				new_value := make([]interface{}, len(current_value))
				copy(new_value, current_value)
				new_value = append(new_value, value)
				current_store[skey] = new_value
				log.Default().Print(current_value, new_value, current_store[skey])
				new_mop[2] = value

			}
			new_transaction = append(new_transaction, new_mop)
		}
		new_json, err := json.Marshal(current_store)
		kv.CompareAndSwap(context.Background(), "root", current_store_json, string(new_json), true)

		body = make(map[string]any)
		body["type"] = "txn_ok"
		body["txn"] = new_transaction

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
