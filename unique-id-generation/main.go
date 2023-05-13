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
	EPOCH       int64 = 1616253079084
	MS_PER_HOUR int64 = 3600000
	lastTime    int64 = 0
	counter     int   = 0
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any = make(map[string]any)
		currentTime := time.Now().UnixMilli()
		diff := currentTime - EPOCH
		if currentTime == lastTime {
			counter++
		} else {
			counter = 0
		}
		idSlice := []byte(n.ID())
		idInt := binary.BigEndian.Uint32(idSlice[:4])
		bi := big.NewInt(0)
		diffS := fmt.Sprintf("%064s", fmt.Sprintf("%.64b", diff))
		idS := fmt.Sprintf("%032s", fmt.Sprintf("%.32b", idInt))
		counterS := fmt.Sprintf("%032s", fmt.Sprintf("%.32b", counter))
		bi.SetString(fmt.Sprintf("%s%s%s", diffS, idS, counterS), 2)
		snowflake := fmt.Sprintf("%x", bi)
		lastTime = currentTime
		body["type"] = "generate_ok"
		body["id"] = snowflake
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

	/*currentTime := time.Now().UnixMilli()
	diff := currentTime - EPOCH
	if currentTime == lastTime {
		counter++
	} else {
		counter = 0
	}
	idInt, _ := strconv.ParseUint("0xdeadbeef", 0, 64)
	snowflake := (diff&int64(math.Pow(2, 42)-1))<<22 | (int64(idInt)&int64(math.Pow(2, 11)-1))<<12 | int64(counter)&int64(math.Pow(2, 13)-1)
	fmt.Printf("%b %b %b %b", snowflake, diff, idInt, counter)*/
}
