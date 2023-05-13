package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/big"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	EPOCH    int64 = 1616253079084
	lastTime int64 = 0
	counter  int   = 0
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// inspired by https://en.wikipedia.org/wiki/Snowflake_ID
		currentTime := time.Now().UnixMilli()
		diff := currentTime - EPOCH
		if currentTime == lastTime {
			counter++
		} else {
			counter = 0
		}
		lastTime = currentTime

		idSlice := []byte(n.ID())
		idInt := binary.BigEndian.Uint32(idSlice[:4])
		// ensures min / max lengths, 0 padded
		diffS := fmt.Sprintf("%064s", fmt.Sprintf("%.64b", diff))       // allocate 64 bits, current times use ~59
		idS := fmt.Sprintf("%032s", fmt.Sprintf("%.32b", idInt))        // allocate 32 bits, will use all
		counterS := fmt.Sprintf("%032s", fmt.Sprintf("%.32b", counter)) // allocate 32 bits, can handle load of 2^32 generations per ms

		bi := big.NewInt(0)
		bi.SetString(fmt.Sprintf("%s%s%s", diffS, idS, counterS), 2)
		snowflake := fmt.Sprintf("%x", bi) // hex encode

		var body map[string]any = make(map[string]any)
		body["type"] = "generate_ok"
		body["id"] = snowflake
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
