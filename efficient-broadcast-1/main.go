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
	valMutex    sync.Mutex
)

func main() {
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

		_, hasBroadcasted := broadcasted[body["message"]]
		if !hasBroadcasted {
			neighbours := getNeighbours(n.NodeIDs(), index(n.NodeIDs(), n.ID()))
			newBody := body
			for _, neighbour := range neighbours {
				neighbour := neighbour
				go func() {
					err := n.Send(neighbour, newBody)
					for err != nil {
						err = n.Send(neighbour, newBody)
					}
				}()
			}
			broadcasted[body["message"]] = true
		}
		valMutex.Unlock()

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

		valMutex.Lock()
		neighbours := getNeighbours(n.NodeIDs(), index(n.NodeIDs(), n.ID()))
		for k := range received {
			_, hasBroadcast := broadcasted[k]
			if hasBroadcast {
				continue
			}
			for _, neighbour := range neighbours {
				neighbour := neighbour
				newBody := body
				go func() {
					err := n.Send(neighbour, newBody)
					for err != nil {
						err = n.Send(neighbour, newBody)
					}
				}()
			}
			broadcasted[k] = true
		}
		valMutex.Unlock()

		body = make(map[string]any)
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func getNeighbours(list []string, index int) []string {
	var neighbours []string
	if index > 0 {
		neighbours = append(neighbours, list[(index-1)/5])
	}
	for i := 0; i < 5; i++ {
		if (5*index + i) < len(list) {
			neighbours = append(neighbours, list[5*index+i])
		}
	}
	return neighbours
}

// why no built-in :(
func index(slice []string, item string) int {
	for i := range slice {
		if slice[i] == item {
			return i
		}
	}
	return -1
}
