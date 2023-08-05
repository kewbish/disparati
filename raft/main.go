package main

import (
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NodeState string

const (
	Leader    NodeState = "leader"
	Candidate NodeState = "candidate"
	Follower  NodeState = "follower"
)

var timeout time.Duration = 500 * time.Millisecond

func main() {
	n := maelstrom.NewNode()
	var mx sync.Mutex
	mapState := make(map[any]any)
	nodeState := Follower
	electionTimeout := 2 * time.Second
	electionDeadline := time.Now()
	term := 0
	// writeAheadLog := RaftLog.NewRaftlog(n)

	advanceTerm := func(newTerm int) error {
		mx.Lock()
		if newTerm < term {
			mx.Unlock()
			return errors.New("Provided term is before current term")
		} else {
			term = newTerm
			mx.Unlock()
		}
		return nil
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			tick := time.Tick(1 * time.Second)
			for range tick {
				mx.Lock()
				if electionDeadline.Before(time.Now()) {
					if nodeState != Leader {
						nodeState = Candidate
						advanceTerm(term + 1)
						log.Default().Printf("Became candidate for term %d", term)
					}
					electionDeadline = time.Now().Add(electionTimeout + (time.Duration(int(rand.Float64()*1000)) * time.Millisecond))
				}
				mx.Unlock()
			}
		}()

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		applyRead := func(mapState map[any]any, key any) (value any, err error) {
			if v, ok := mapState[key]; ok {
				return v, nil
			} else {
				maelstromError := maelstrom.NewRPCError(404, "Key not found")
				return "", maelstromError
			}
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]

		value, err := applyRead(mapState, key)
		if err != nil {
			return err
		}

		body = make(map[string]any)
		body["type"] = "read_ok"
		body["value"] = value

		return n.Reply(msg, body)
	})

	n.Handle("write", func(msg maelstrom.Message) error {
		applyWrite := func(mapState map[any]any, key any, value any) (newMapState map[any]any, err error) {
			newMapState = make(map[any]any)
			for k, v := range mapState {
				newMapState[k] = v
			}
			newMapState[key] = value
			return newMapState, nil
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]
		value := body["value"]

		newMapState, err := applyWrite(mapState, key, value)
		if err != nil {
			return err
		}
		mx.Lock()
		mapState = newMapState
		mx.Unlock()

		body = make(map[string]any)
		body["type"] = "write_ok"

		return n.Reply(msg, body)
	})

	n.Handle("cas", func(msg maelstrom.Message) error {
		applyCAS := func(mapState map[any]any, key any, from any, to any) (newMapState map[any]any, err error) {
			if v, ok := mapState[key]; ok {
				if v != from {
					return mapState, maelstrom.NewRPCError(400, "Value is not equal to from")
				}
				newMapState = make(map[any]any)
				for k, v := range mapState {
					newMapState[k] = v
				}
				newMapState[key] = to
				return newMapState, nil
			} else {
				return mapState, maelstrom.NewRPCError(404, "Key not found")
			}
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"]
		from := body["from"]
		to := body["to"]

		newMapState, err := applyCAS(mapState, key, from, to)
		if err != nil {
			return err
		}
		mx.Lock()
		mapState = newMapState
		mx.Unlock()

		body = make(map[string]any)
		body["type"] = "cas_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type RaftLogEntry struct {
	term      int
	operation any
}
type RaftLog struct {
	node *maelstrom.Node
	log  []RaftLogEntry
}

func (RaftLog) NewRaftlog(node *maelstrom.Node) *RaftLog {
	newRaftLog := new(RaftLog)
	newRaftLog.node = node
	newRaftLog.log = []RaftLogEntry{{term: 0, operation: nil}}
	return newRaftLog
}

func (r RaftLog) Get(i int) RaftLogEntry {
	return r.log[i-1]
}

func (r *RaftLog) AppendEntries(entries []RaftLogEntry) {
	for _, entry := range entries {
		r.log = append(r.log, entry)
	}
}

func (r RaftLog) Last(i int) RaftLogEntry {
	return r.log[len(r.log)-1]
}
