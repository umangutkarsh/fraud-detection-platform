package messaging

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestSASLMechanismsAreAccepted(t *testing.T) {
	cases := map[string]string{
		"":              "",
		"plain":         "PLAIN",
		"scram-sha-256": "SCRAM-SHA-256",
		"scram-sha-512": "SCRAM-SHA-512",
	}
	for in, wantName := range cases {
		t.Run(in, func(t *testing.T) {
			p, err := NewKafkaPublisher(KafkaOptions{
				Brokers: []string{"localhost:9092"}, Topic: "t",
				SASLMechanism: in, SASLUsername: "u", SASLPassword: "p",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Close() }()
			tr := p.w.Transport.(*kafka.Transport)
			if wantName == "" {
				if tr.SASL != nil {
					t.Fatal("no SASL expected")
				}
				return
			}
			if tr.SASL == nil || tr.SASL.Name() != wantName {
				t.Fatalf("mechanism = %v, want %s", tr.SASL, wantName)
			}
		})
	}
}

func TestUnknownSASLMechanismIsRejected(t *testing.T) {
	if _, err := NewKafkaPublisher(KafkaOptions{Brokers: []string{"x:9092"}, Topic: "t", SASLMechanism: "bogus"}); err == nil {
		t.Fatal("an unknown mechanism must fail at startup")
	}
}
