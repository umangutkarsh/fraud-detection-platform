package pipeline

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/segmentio/kafka-go"
)

func shardFor(msg kafka.Message, shards int) int {
	h := fnv.New32a()
	if len(msg.Key) > 0 {
		_, _ = h.Write(msg.Key)
	} else {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(msg.Partition))
		_, _ = h.Write(b[:])
	}
	return int(h.Sum32() % uint32(shards))
}
