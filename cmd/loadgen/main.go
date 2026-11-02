// loadgen drives the whole platform through its real front door: signed
// requests to the gateway's POST /v1/transactions, with the fraud decision
// read back over GET /v1/transactions/{id}.
//
// It measures two things:
//
//	submit latency    how long the gateway takes to accept a transaction
//	decision latency  how long until the processor's decision is readable
//	                  (submit -> Kafka -> processor -> Redis -> gateway)
//
// Requests are issued on a fixed schedule (open loop) and latency is measured
// from the time each request was MEANT to start. If the system falls behind,
// the delay shows up in the numbers instead of silently lowering the rate
// (the "coordinated omission" mistake that makes slow systems look fast).
//
//	export JWT_SECRET=...   # same secret the gateway uses
//	go run ./cmd/loadgen -rate 500 -duration 30s
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"
)

type config struct {
	baseURL     string
	rate        int
	duration    time.Duration
	workers     int
	merchants   int
	users       int
	fraudRatio  float64
	sample      int
	issuer      string
	audience    string
	pollTimeout time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.baseURL, "url", "http://localhost:8080", "gateway base URL")
	flag.IntVar(&c.rate, "rate", 500, "target requests per second")
	flag.DurationVar(&c.duration, "duration", 30*time.Second, "how long to generate load")
	flag.IntVar(&c.workers, "workers", 200, "concurrent request workers")
	flag.IntVar(&c.merchants, "merchants", 20, "distinct merchants (each gets its own token and rate-limit bucket)")
	flag.IntVar(&c.users, "users", 5000, "distinct users")
	flag.Float64Var(&c.fraudRatio, "fraud-ratio", 0.05, "fraction of jobs sent as a rapid same-user pair (trips the velocity rule)")
	flag.IntVar(&c.sample, "sample", 50, "also measure decision latency for 1 in N ordinary requests")
	flag.StringVar(&c.issuer, "issuer", "api-gateway", "JWT issuer the gateway expects")
	flag.StringVar(&c.audience, "audience", "", "JWT audience, if the gateway requires one")
	flag.DurationVar(&c.pollTimeout, "poll-timeout", 10*time.Second, "give up waiting for a decision after this long")
	flag.Parse()

	secret := os.Getenv("JWT_SECRET")
	if len(secret) < 32 {
		fmt.Fprintln(os.Stderr, "JWT_SECRET must be set (>= 32 bytes) and equal to the gateway's")
		os.Exit(2)
	}
	if c.rate < 1 || c.workers < 1 || c.merchants < 1 || c.users < 1 || c.sample < 1 {
		fmt.Fprintln(os.Stderr, "rate, workers, merchants, users and sample must be >= 1")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, c, []byte(secret)))
}

func run(ctx context.Context, c config, secret []byte) int {
	tokens := make([]string, c.merchants)
	for i := range tokens {
		tokens[i] = mintToken(secret, c.issuer, c.audience, fmt.Sprintf("loadgen-%d", i), c.duration+c.pollTimeout+time.Minute)
	}

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        c.workers + 64,
			MaxIdleConnsPerHost: c.workers + 64,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	runID := time.Now().Format("150405") // keeps transaction IDs unique across runs

	fmt.Printf("loadgen: %d req/s for %s -> %s (%d merchants, %d workers)\n", c.rate, c.duration, c.baseURL, c.merchants, c.workers)

	jobs := make(chan job, c.rate*2)
	col := &collector{}
	pollers := make(chan struct{}, 64) // bounds concurrent decision pollers
	var pollWG, workerWG sync.WaitGroup

	results := make([]*result, c.workers)
	for i := range results {
		results[i] = &result{codes: map[int]int{}}
		workerWG.Add(1)
		go func(r *result) {
			defer workerWG.Done()
			w := &worker{c: c, client: client, tokens: tokens, runID: runID, res: r, col: col, pollers: pollers, pollWG: &pollWG, ctx: ctx}
			w.run(jobs)
		}(results[i])
	}

	// Producer: issue jobs on a fixed schedule, independent of how fast the
	// system answers. A full queue means the load generator itself cannot keep
	// up, which is reported as "dropped" and not hidden.
	start := time.Now()
	issued, dropped := 0, 0
	ticker := time.NewTicker(5 * time.Millisecond)
produce:
	for {
		select {
		case <-ctx.Done():
			break produce
		case now := <-ticker.C:
			elapsed := min(now.Sub(start), c.duration)
			target := int(elapsed.Seconds() * float64(c.rate))
			for issued < target {
				at := start.Add(time.Duration(float64(issued) / float64(c.rate) * float64(time.Second)))
				select {
				case jobs <- job{at: at, seq: issued}:
				default:
					dropped++
				}
				issued++
			}
			if now.Sub(start) >= c.duration {
				break produce
			}
		}
	}
	ticker.Stop()
	close(jobs)
	workerWG.Wait()
	sendEnd := time.Since(start)
	pollWG.Wait()

	report(c, results, col, issued, dropped, sendEnd)
	return 0
}

type job struct {
	at  time.Time // when this request was scheduled to start
	seq int
}

type result struct {
	lat   []time.Duration
	codes map[int]int
	errs  int // transport errors and timeouts
}

type collector struct {
	mu        sync.Mutex
	decisions []time.Duration
	status    map[string]int
}

func (c *collector) record(d time.Duration, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == nil {
		c.status = map[string]int{}
	}
	c.status[status]++
	if status == "approved" || status == "under_review" {
		c.decisions = append(c.decisions, d)
	}
}

type worker struct {
	c       config
	client  *http.Client
	tokens  []string
	runID   string
	res     *result
	col     *collector
	pollers chan struct{}
	pollWG  *sync.WaitGroup
	ctx     context.Context
}

func (w *worker) run(jobs <-chan job) {
	for j := range jobs {
		token := w.tokens[j.seq%len(w.tokens)]
		user := fmt.Sprintf("user-%d", rand.IntN(w.c.users))

		if rand.Float64() < w.c.fraudRatio {
			// Two payments from one user back to back: the second should be
			// flagged by the velocity rule.
			w.send(token, fmt.Sprintf("tx-%s-%da", w.runID, j.seq), user, 5_00, "GB", j.at, false)
			w.send(token, fmt.Sprintf("tx-%s-%db", w.runID, j.seq), user, 5_00, "GB", time.Now(), true)
			continue
		}
		w.send(token, fmt.Sprintf("tx-%s-%d", w.runID, j.seq), user, int64(100+rand.IntN(50_000)), "GB", j.at, j.seq%w.c.sample == 0)
	}
}

func (w *worker) send(token, id, user string, pence int64, country string, intended time.Time, watch bool) {
	body := fmt.Sprintf(`{"id":%q,"user_id":%q,"amount_minor":%d,"currency":"GBP","country":%q}`, id, user, pence, country)
	req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, w.c.baseURL+"/v1/transactions", bytes.NewReader([]byte(body)))
	if err != nil {
		w.res.errs++
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	lat := time.Since(intended)
	if err != nil {
		w.res.errs++
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	w.res.lat = append(w.res.lat, lat)
	w.res.codes[resp.StatusCode]++

	if watch && resp.StatusCode == http.StatusAccepted {
		select {
		case w.pollers <- struct{}{}:
			w.pollWG.Add(1)
			go func() {
				defer w.pollWG.Done()
				defer func() { <-w.pollers }()
				w.awaitDecision(token, id, intended)
			}()
		default: // too many pollers already; skip this sample
		}
	}
}

// awaitDecision polls until the decision is readable and records how long the
// whole submit -> decision path took.
func (w *worker) awaitDecision(token, id string, since time.Time) {
	deadline := time.Now().Add(w.c.pollTimeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(w.ctx, http.MethodGet, w.c.baseURL+"/v1/transactions/"+id, nil)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := w.client.Do(req)
		if err != nil {
			w.col.record(0, "poll_error")
			return
		}
		var out struct {
			Status string `json:"status"`
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		_ = json.Unmarshal(raw, &out)

		switch {
		case resp.StatusCode == http.StatusOK && out.Status != "":
			w.col.record(time.Since(since), out.Status)
			return
		case resp.StatusCode == http.StatusAccepted: // still pending
			time.Sleep(20 * time.Millisecond)
		default:
			w.col.record(0, fmt.Sprintf("http_%d", resp.StatusCode))
			return
		}
	}
	w.col.record(0, "timed_out")
}

func report(c config, results []*result, col *collector, issued, dropped int, sendEnd time.Duration) {
	var lat []time.Duration
	codes := map[int]int{}
	errs := 0
	for _, r := range results {
		lat = append(lat, r.lat...)
		errs += r.errs
		for code, n := range r.codes {
			codes[code] += n
		}
	}
	done := len(lat)

	fmt.Println()
	fmt.Printf("scheduled      %d jobs, %d dropped by the generator\n", issued, dropped)
	fmt.Printf("completed      %d requests in %.1fs (%.0f req/s achieved, %d target)\n",
		done, sendEnd.Seconds(), float64(done)/sendEnd.Seconds(), c.rate)

	keys := make([]int, 0, len(codes))
	for k := range codes {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	fmt.Print("responses     ")
	for _, k := range keys {
		fmt.Printf(" %d: %d", k, codes[k])
	}
	fmt.Printf("   transport errors: %d\n", errs)

	fmt.Printf("submit latency  %s\n", percentiles(lat))

	col.mu.Lock()
	defer col.mu.Unlock()
	fmt.Printf("decision latency %s  (%d sampled)\n", percentiles(col.decisions), len(col.decisions))
	names := make([]string, 0, len(col.status))
	for k := range col.status {
		names = append(names, k)
	}
	sort.Strings(names)
	fmt.Print("decisions      ")
	for _, k := range names {
		fmt.Printf(" %s: %d", k, col.status[k])
	}
	fmt.Println()

	if codes[http.StatusTooManyRequests] > 0 {
		fmt.Println("\nnote: 429 responses mean a rate limit was hit. Raise the gateway's limits for a pure throughput test")
		fmt.Println("      (see docker-compose.loadtest.yml), or add -merchants.")
	}
}

func percentiles(d []time.Duration) string {
	if len(d) == 0 {
		return "no samples"
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	at := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	ms := func(x time.Duration) float64 { return float64(x.Microseconds()) / 1000 }
	return fmt.Sprintf("p50 %.1fms  p90 %.1fms  p99 %.1fms  max %.1fms", ms(at(0.50)), ms(at(0.90)), ms(at(0.99)), ms(s[len(s)-1]))
}

// mintToken signs an HS256 JWT with the standard library only, so the load
// generator needs no dependencies.
func mintToken(secret []byte, issuer, audience, subject string, ttl time.Duration) string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := map[string]any{
		"sub": subject, "iss": issuer, "exp": time.Now().Add(ttl).Unix(),
		"scope": "transactions:write transactions:read",
	}
	if audience != "" {
		claims["aud"] = audience
	}
	raw, _ := json.Marshal(claims)
	signing := header + "." + enc.EncodeToString(raw)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signing))
	return signing + "." + enc.EncodeToString(mac.Sum(nil))
}
