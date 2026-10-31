package pipeline

import (
	"context"
	"encoding/binary"
	"fraud-detection-platform/internal/processor/metrics"
	"hash/fnv"
	"time"

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

func (p *Pipeline) worker(id int, in <-chan kafka.Message) {
	defer p.workerWG.Done()
	p.log.Info("worker started", "worker", id)
	defer p.log.Info("worker stopped", "worker", id)

	for msg := range in {
		if err := p.proc.Process(p.workCtx, msg); err != nil {
			// No terminal outcome (shutdown, or the DLQ is down). Leave the
			// offset uncommitted: it will be redelivered, never lost.
			p.log.Warn("message left uncommitted", "partition", msg.Partition, "offset", msg.Offset, "error", err)
			continue
		}
		p.tracker.Complete(&msg)
	}
}

func (p *Pipeline) commitLoop() {
	defer p.auxWG.Done()

	ticker := time.NewTicker(p.cfg.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.flushCommits()
		case <-p.auxStop:
			p.flushCommits() // final flush on shutdown
			return
		}
	}
}

func (p *Pipeline) flushCommits() {
	msgs := p.tracker.Drain()
	if len(msgs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.reader.CommitMessages(ctx, msgs...); err != nil {
		metrics.CommitFailures.Inc()
		p.log.Error("offset commit failed; will retry", "error", err)
		p.tracker.Restore(msgs)
	}
}
