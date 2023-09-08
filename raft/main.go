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

// TODO - fix deadlocks in election process!

func main() {
	n := maelstrom.NewNode()
	var mx sync.Mutex
	mapState := make(map[any]any)
	nodeState := Follower
	electionTimeout := 2 * time.Second
	electionDeadline := time.Now()
	stepDownDeadline := time.Now()
	term := 0
	writeAheadLog := NewRaftlog(n)
	votedFor := ""

	advanceTerm := func(newTerm int) error {
		mx.Lock()
		if newTerm < term {
			mx.Unlock()
			return errors.New("Provided term is before current term")
		} else {
			term = newTerm
			votedFor = ""
			mx.Unlock()
		}
		return nil
	}

	maybeBecomeFollower := func(considerTerm int) {
		if term < considerTerm {
			log.Default().Printf("Became follower, remote term %d higher than local term %d", considerTerm, term)
			advanceTerm(considerTerm)
			mx.Lock()
			nodeState = Follower
			mx.Unlock()
		}
	}

	advanceElectionDeadline := func() {
		electionDeadline = time.Now().Add(electionTimeout + (time.Duration(int(rand.Float64()*1000)) * time.Millisecond))
	}

	advanceStepDownDeadline := func() {
		stepDownDeadline = time.Now().Add(electionTimeout + (time.Duration(int(rand.Float64()*1000)) * time.Millisecond))
	}

	startElection := func() {
		votes := make(map[string]bool)
		votes[n.ID()] = true
		votedFor = n.ID()
		currentTerm := term
		for _, nodeID := range n.NodeIDs() {
			request := map[string]any{"type": "request_vote", "term": term, "candidate_id": n.ID, "last_log_index": len(writeAheadLog.log), "last_log_term": writeAheadLog.Last().term}
			n.RPC(n.ID(), request, func(msg maelstrom.Message) error {
				advanceStepDownDeadline()
				var body map[string]any
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					return err
				}
				responseTerm := body["term"].(int)
				maybeBecomeFollower(responseTerm)
				voteGranted := body["vote_granted"].(bool)
				if nodeState == Candidate && term == currentTerm && term == responseTerm && voteGranted {
					votes[nodeID] = true
				}
				if len(n.NodeIDs())/2+1 <= len(votes) {
					log.Default().Printf("Became leader for term %d with votes %v", term, votes)
					mx.Lock()
					nodeState = Leader
					mx.Unlock()
					advanceStepDownDeadline()
				}
				return nil
			})
			if nodeState == Leader {
				break
			}
		}
	}

	n.Handle("init", func(msg maelstrom.Message) error {
		go func() {
			tick := time.Tick(500 * time.Millisecond)
			for range tick {
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
				if electionDeadline.Before(time.Now()) {
					if nodeState != Leader {
						mx.Lock()
						nodeState = Candidate
						mx.Unlock()
						advanceTerm(term + 1)
						advanceElectionDeadline()
						advanceStepDownDeadline()
						log.Default().Printf("Became candidate for term %d", term)
						startElection()
					} else {
						advanceElectionDeadline()
					}
				}
			}
		}()

		go func() {
			tick := time.Tick(500 * time.Millisecond)
			for range tick {
				mx.Lock()
				if nodeState == Leader && stepDownDeadline.Before(time.Now()) {
					log.Default().Printf("Stepping down, have not received any pings in / acks recently")
					nodeState = Follower
					advanceElectionDeadline()
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

	n.Handle("request_vote", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		requestedTerm := body["term"].(int)
		candidateId := body["candidate_id"].(string)
		lastLogIndex := body["last_log_index"].(int)
		lastLogTerm := body["last_log_term"].(int)
		voteGranted := false

		maybeBecomeFollower(requestedTerm)
		if requestedTerm < term {
			log.Default().Printf("Candidate term %d lower than current term %d", requestedTerm, term)
		} else if votedFor != "" {
			log.Default().Printf("Voted for %s already in current term", votedFor)
		} else if lastLogTerm < writeAheadLog.Last().term {
			log.Default().Printf("Candidate WAL is still on term %d, current WAL on term %d", lastLogTerm, writeAheadLog.Last().term)
		} else if lastLogTerm == writeAheadLog.Last().term && lastLogIndex < len(writeAheadLog.log) {
			log.Default().Print("Candidate WAL is shorter than current WAL")
		} else {
			log.Default().Printf("Granting vote to candidate %s", candidateId)
			voteGranted = true
			mx.Lock()
			votedFor = candidateId
			advanceElectionDeadline()
			mx.Unlock()
		}

		body = make(map[string]any)
		body["type"] = "request_vote_res"
		body["term"] = term
		body["vote_granted"] = voteGranted

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

func NewRaftlog(node *maelstrom.Node) *RaftLog {
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

func (r RaftLog) Last() RaftLogEntry {
	return r.log[len(r.log)-1]
}
