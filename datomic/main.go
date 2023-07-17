package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var timeout time.Duration = 500 * time.Millisecond

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	var mx sync.Mutex
	thunkMap := new(Map)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		transaction := body["txn"].([]interface{})
		mx.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		value, err := kv.Read(ctx, "root")
		if err != nil {
			return err
		}
		thunkMap.fromJSON(n, kv, value.([]byte))

		newThunkMap := Map{n, kv, make(map[any]*Thunk)}
		new_transaction := make([][]interface{}, 0)
		for _, mop := range transaction {
			mop := mop
			mop_interface := mop.([]interface{})
			action, key, value := mop_interface[0], mop_interface[1], mop_interface[2]

			new_mop := make([]interface{}, 3)
			new_mop[0] = action
			new_mop[1] = key
			switch action {
			case "r":
				new_mop[2] = newThunkMap.internalMap[key]
			case "append":
				new_value := make([]interface{}, len(newThunkMap.internalMap[key].value.([]any)))
				copy(new_value, newThunkMap.internalMap[key].value.([]any))
				new_value = append(new_value, value)
				thunk := newThunk(n, kv, fmt.Sprintf("%s-%d", n.ID(), globalIDG.newID()), new_value, false)
				newInternalMap := make(map[any]*Thunk)
				for k, v := range newThunkMap.internalMap {
					newInternalMap[k] = v
				}
				newInternalMap[key] = thunk
				newThunkMap.internalMap = newInternalMap
			}
			new_transaction = append(new_transaction, new_mop)
		}

		newThunkMap.save()

		ctx, cancel_another := context.WithTimeout(context.Background(), timeout)
		kv.CompareAndSwap(context.Background(), "root", thunkMap.toJSON(), newThunkMap.toJSON(), true) // TODO - add retry
		defer cancel_another()
		mx.Unlock()

		body = make(map[string]any)
		body["type"] = "txn_ok"
		body["txn"] = new_transaction

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type IDGenerator struct {
	lock  *sync.Mutex
	index int64
}

var globalIDG IDGenerator = IDGenerator{new(sync.Mutex), 0}

func (idg IDGenerator) newID() int64 {
	idg.lock.Lock()
	idg.index += 1
	idg.lock.Unlock()
	return idg.index
}

type Thunk struct {
	node  *maelstrom.Node
	kv    *maelstrom.KV
	id    string
	value any
	saved bool
}

func newThunk(node *maelstrom.Node, kv *maelstrom.KV, id string, value any, saved bool) *Thunk {
	thunk := new(Thunk)
	*thunk = Thunk{node, kv, id, value, saved}
	return thunk
}

func (t Thunk) getThunkID() string {
	if t.id != "" {
		return t.id
	}
	return fmt.Sprintf("%s-%d", (*t.node).ID(), globalIDG.newID())
}

func (t Thunk) getThunkValue() any {
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	value, err := t.kv.Read(contextWithTimeout, t.id)
	if err != nil {
		return nil
	}
	t.value = value
	return value
}

func (t Thunk) saveThunkValue() {
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := t.kv.Write(contextWithTimeout, t.id, t.value)
	if err == nil {
		t.saved = true
	}
}

type Map struct {
	node        *maelstrom.Node
	kv          *maelstrom.KV
	internalMap map[any]*Thunk
}

func (Map) fromJSON(node *maelstrom.Node, kv *maelstrom.KV, jsonString []byte) *Map {
	newMap := new(Map)
	newMap.node = node
	newMap.internalMap = make(map[any]*Thunk)
	var pairs [][]any
	json.Unmarshal(jsonString, &pairs)
	for _, pair := range pairs {
		newMap.internalMap[pair[0]] = newThunk(node, kv, pair[1].(string), nil, true)
	}
	return newMap
}

func (m Map) toJSON() []byte {
	pairs := make([][]any, 0)
	for k, v := range m.internalMap {
		pairs = append(pairs, []any{k, v.id})
	}
	bytes, _ := json.Marshal(pairs)
	return bytes
}

func (m Map) save() {
	for _, v := range m.internalMap {
		v.saveThunkValue()
	}
}
