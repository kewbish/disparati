package main

import (
	"encoding/json"
	"log"
	"math"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	incs := make(map[interface{}]float64)
	decs := make(map[interface{}]float64)
	var valueMutex sync.RWMutex

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			tick := time.Tick(500 * time.Millisecond)
			for range tick {
				valueMutex.RLock()
				keys := make([]interface{}, 0, len(incs)+len(decs))
				vals := make([]float64, 0, len(incs)+len(decs))
				for k, v := range incs {
					keys = append(keys, k)
					vals = append(vals, v)
				}
				for k, v := range decs {
					keys = append(keys, k)
					vals = append(vals, v)
				}
				valueMutex.RUnlock()

				for _, neighbour := range n.NodeIDs() {
					if neighbour != n.ID() {
						n.RPC(neighbour, map[string]any{"type": "replicate", "keys": keys, "vals": vals}, func(msg maelstrom.Message) error {
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
		var finalCounter float64 = 0
		for _, v := range incs {
			finalCounter += v
		}
		for _, v := range decs {
			finalCounter += v
		}
		valueMutex.RUnlock()

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

		valueMutex.Lock()
		delta := (body["delta"]).(float64)
		if delta > 0 {
			if _, exists := incs[msg.Src]; !exists {
				incs[msg.Src] = 0
			}
			incs[msg.Src] += delta
		} else {
			if _, exists := decs[msg.Src]; !exists {
				decs[msg.Src] = 0
			}
			decs[msg.Src] += delta
		}
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
		for i := range body["keys"].([]interface{}) {
			incs[(body["keys"]).([]interface{})[i]] = math.Max(float64(incs[(body["keys"]).([]interface{})[i]]), float64((body["vals"]).([]interface{})[i].(float64)))
			decs[(body["keys"]).([]interface{})[i]] = math.Min(float64(decs[(body["keys"]).([]interface{})[i]]), float64((body["vals"]).([]interface{})[i].(float64)))
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
