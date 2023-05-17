package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

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
		_, isSingle := body["message"]
		if isSingle {
			received[body["message"]] = true
		} else {
			for m := range body["messages"].([]interface{}) {
				received[m] = true
			}
		}
		valMutex.Unlock()

		go func() {
			body := body
			for {
				var toBroadcast []interface{}
				for k := range received {
					_, hasBroadcasted := broadcasted[body["message"]]
					if !hasBroadcasted {
						toBroadcast = append(toBroadcast, k)
					}
				}
				neighbours := getNeighbours(n.NodeIDs(), index(n.NodeIDs(), n.ID()))
				newBody := body
				delete(newBody, "message")
				newBody["messages"] = toBroadcast
				for _, neighbour := range neighbours {
					neighbour := neighbour
					go func() {
						err := n.Send(neighbour, newBody)
						for err != nil {
							time.Sleep(time.Second)
							err = n.Send(neighbour, newBody)
						}
					}()
				}
				for m := range toBroadcast {
					broadcasted[m] = true
				}
				time.Sleep(300 * time.Millisecond)
			}
		}()

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
		body := make(map[string]any)
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
		neighbours = append(neighbours, list[(index-1)/10])
	}
	for i := 0; i < 10; i++ {
		if (10*index + i + 1) < len(list) {
			neighbours = append(neighbours, list[10*index+i+1])
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
