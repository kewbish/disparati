package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	store := make(map[interface{}][]interface{})
	var valueMutex sync.RWMutex

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		transaction := body["txn"].([]interface{})
		new_transaction := make([][]interface{}, 0)

		valueMutex.Lock()
		for _, mop := range transaction {
			mop := mop
			mop_interface := mop.([]interface{})
			action, key, value := mop_interface[0], mop_interface[1], mop_interface[2]

			current_value, exists := store[key]
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
				store[key] = append(store[key], value)
				new_mop[2] = value

			}
			new_transaction = append(new_transaction, new_mop)
		}
		valueMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "txn_ok"
		body["txn"] = new_transaction

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
