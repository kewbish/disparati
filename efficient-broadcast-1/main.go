package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/btree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	received    map[interface{}]bool
	broadcasted map[interface{}]bool
	valMutex    sync.Mutex
	tree        btree.Tree
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
			neighbours := getNeighbours(n.ID())
			newBody := body
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

		list := n.NodeIDs()
		tree := btree.NewWithIntComparator(len(list))
		for i := 0; i < len(list); i++ {
			tree.Put(i, list[i])
		}

		go func() {
			body := make(map[string]any)
			body["type"] = "topology_ok"
			n.Reply(msg, body)
		}()

		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func getNeighbours(index string) []string {
	cNode := tree.GetNode(index)

	var neighbours []string
	if cNode.Parent != nil {
		neighbours = append(neighbours, cNode.Parent.Entries[0].Value.(string))
	}

	for _, children := range cNode.Children {
		for _, entry := range children.Entries {
			neighbours = append(neighbours, entry.Value.(string))
		}
	}

	return neighbours
}
