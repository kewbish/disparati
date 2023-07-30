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
	thunkCache := make(map[string]*Thunk)
	globalIDG := IDGenerator{n, new(sync.Mutex), 0}

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		transaction := body["txn"].([]interface{})

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		value, err := kv.Read(ctx, "root")
		if err != nil {
			value = ""
		}
		thunkMap = thunkMap.fromJSON(n, kv, value.(string))

		mx.Lock()

		wasAppend := false
		newThunkMap := Map{n, kv, thunkMap.internalMap, thunkMap.id}
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
				if _, ok := newThunkMap.getThunkValue(thunkCache)[key]; ok {
					new_mop[2] = newThunkMap.getThunkValue(thunkCache)[key].getThunkValue()
				} else {
					new_mop[2] = make([]interface{}, 0)
				}
			case "append":
				wasAppend = true
				new_mop[2] = value
				var new_value []interface{}
				if _, ok := newThunkMap.getThunkValue(thunkCache)[key]; ok {
					thunkValue := newThunkMap.getThunkValue(thunkCache)[key].getThunkValue()
					new_value = make([]interface{}, len(thunkValue.([]any)))
					copy(new_value, thunkValue.([]any))
					new_value = append(new_value, value)
				} else {
					new_value = []interface{}{value}
				}
				thunk := newThunk(n, kv, globalIDG.newID(), new_value, false)
				newInternalMap := make(map[any]*Thunk)
				for k, v := range newThunkMap.getThunkValue(thunkCache) {
					newInternalMap[k] = v
				}
				newInternalMap[key] = thunk
				newThunkMap.internalMap = newInternalMap
			}
			new_transaction = append(new_transaction, new_mop)
		}

		newThunkMap.save(globalIDG.newID(), thunkCache)
		if wasAppend {
			for {
				ctx, cancel_another := context.WithTimeout(context.Background(), timeout)
				err := kv.CompareAndSwap(ctx, "root", value, newThunkMap.id, true)
				defer cancel_another()
				if err == nil {
					break
				}
			}
		}

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
	node  *maelstrom.Node
	lock  *sync.Mutex
	index int64
}

func (idg *IDGenerator) newID() string {
	idg.lock.Lock()
	idg.index += 1
	idg.lock.Unlock()
	return fmt.Sprintf("%s-%d", (*idg.node).ID(), idg.index)
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

func (t *Thunk) getThunkValue() any {
	if t.value != nil {
		return t.value
	}
	var value any
	var err error
	for {
		contextWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		value, err = t.kv.Read(contextWithTimeout, t.id)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	t.value = value
	return value
}

func (t *Thunk) saveThunkValue() {
	if t.saved {
		return
	}
	contextWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := t.kv.Write(contextWithTimeout, t.id, t.getThunkValue())
	if err == nil {
		t.saved = true
	} else {
		log.Default().Print(err)
	}
}

// like a thunk, but Go's "subclassing" is awkward so keeping this a separate struct
type Map struct {
	node        *maelstrom.Node
	kv          *maelstrom.KV
	internalMap map[any]*Thunk
	id          string
}

func (Map) fromJSON(node *maelstrom.Node, kv *maelstrom.KV, id string) *Map {
	newMap := new(Map)
	newMap.node = node
	newMap.kv = kv
	newMap.internalMap = make(map[any]*Thunk)
	newMap.id = id
	return newMap
}

func (m *Map) save(id string, thunkCache map[string]*Thunk) {
	for _, v := range m.internalMap {
		v.saveThunkValue()
	}
	// create and save thunk on-demand
	pairs := make([][]any, 0)
	for k, v := range m.internalMap {
		pairs = append(pairs, []any{k, v.id})
	}
	bytes, _ := json.Marshal(pairs)

	var mapAsThunk *Thunk
	if _, ok := thunkCache[id]; ok {
		mapAsThunk = thunkCache[id]
	} else {
		mapAsThunk = newThunk(m.node, m.kv, id, string(bytes), false)
	}
	mapAsThunk.saveThunkValue()
	m.id = id
}

func (m *Map) getThunkValue(thunkCache map[string]*Thunk) map[any]*Thunk {
	if len(m.internalMap) != 0 || m.id == "" {
		return m.internalMap
	}

	var value any
	var err error
	for {
		contextWithTimeout, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		value, err = m.kv.Read(contextWithTimeout, m.id)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}

	newInternalMap := make(map[any]*Thunk)
	pairs := make([][]any, 0)
	if len(value.(string)) != 0 {
		json.Unmarshal([]byte(value.(string)), &pairs)
	}
	for _, pair := range pairs {
		if _, ok := thunkCache[pair[1].(string)]; ok {
			newInternalMap[pair[0]] = thunkCache[pair[1].(string)]
		} else {
			newInternalMap[pair[0]] = newThunk(m.node, m.kv, pair[1].(string), nil, true)
		}
	}

	m.internalMap = newInternalMap
	return newInternalMap
}
