package pipeline

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func msg(partition int, offset int64) *kafka.Message {
	return &kafka.Message{Partition: partition, Offset: offset}
}

func committed(t *testing.T, tr *OffsetTracker) map[int]int64 {
	t.Helper()
	out := map[int]int64{}
	for _, m := range tr.Drain() {
		out[m.Partition] = m.Offset
	}
	return out
}

func TestInOrderCompletionCommitsLatest(t *testing.T) {
	tr := NewOffsetTracker()
	for offset := int64(0); offset < 3; offset++ {
		tr.Track(msg(0, offset))
	}
	for offset := int64(0); offset < 3; offset++ {
		tr.Complete(msg(0, offset))
	}
	if got := committed(t, tr); got[0] != 2 {
		t.Fatalf("want commit of offset 2, got %v", got)
	}
	if tr.Pending() != 0 {
		t.Fatalf("pending = %d, want 0", tr.Pending())
	}
}

// The scenario that lost data in the original design: 102 finishes first.
func TestOutOfOrderCompletionNeverSkipsAnInFlightOffset(t *testing.T) {
	tr := NewOffsetTracker()
	for offset := int64(100); offset <= 102; offset++ {
		tr.Track(msg(0, offset))
	}

	tr.Complete(msg(0, 102))
	tr.Complete(msg(0, 101))
	if got := committed(t, tr); len(got) != 0 {
		t.Fatalf("offset 100 is still in flight; nothing may be committed, got %v", got)
	}

	tr.Complete(msg(0, 100))
	if got := committed(t, tr); got[0] != 102 {
		t.Fatalf("once 100 completes the whole run is committable (102), got %v", got)
	}
}

func TestPartitionsAreIndependent(t *testing.T) {
	tr := NewOffsetTracker()
	tr.Track(msg(0, 10))
	tr.Track(msg(1, 20))
	tr.Complete(msg(1, 20))
	got := committed(t, tr)
	if _, ok := got[0]; ok || got[1] != 20 {
		t.Fatalf("only partition 1 may commit, got %v", got)
	}
}

func TestRestoreAfterFailedCommit(t *testing.T) {
	tr := NewOffsetTracker()
	tr.Track(msg(0, 5))
	tr.Complete(msg(0, 5))
	drained := tr.Drain()
	tr.Restore(drained)
	if got := committed(t, tr); got[0] != 5 {
		t.Fatalf("restored commit must be retried, got %v", got)
	}
}

func TestRewindAfterRebalanceDoesNotWedgeCommits(t *testing.T) {
	tr := NewOffsetTracker()
	tr.Track(msg(0, 105))
	tr.Track(msg(0, 106)) // in flight when the partition is revoked
	tr.Track(msg(0, 103)) // reassigned: reader rewound to the committed offset
	tr.Complete(msg(0, 103))
	if got := committed(t, tr); got[0] != 103 {
		t.Fatalf("commits must resume after a rewind, got %v", got)
	}
}

func TestDuplicateCompleteIsHarmless(t *testing.T) {
	tr := NewOffsetTracker()
	tr.Track(msg(0, 1))
	tr.Complete(msg(0, 1))
	tr.Complete(msg(0, 1))
	if tr.Pending() != 0 {
		t.Fatalf("pending = %d", tr.Pending())
	}
}
