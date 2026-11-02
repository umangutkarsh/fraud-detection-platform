package pipeline

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"fraud-detection-platform/internal/gateway/adapter/application/rest/handler"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/middleware"
	"fraud-detection-platform/internal/gateway/adapter/application/rest/routes"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/ratelimit"
	"fraud-detection-platform/internal/gateway/adapter/infrastructure/store"
	"fraud-detection-platform/internal/gateway/domain/port"
	"fraud-detection-platform/internal/gateway/domain/service"
	"fraud-detection-platform/internal/processor/rules"
)

// inProcessKafka stands in for the broker: Publish hands the event straight to
// the real processor, exactly as the consumer would after a fetch.
type inProcessKafka struct{ proc *Processor }

func (k *inProcessKafka) Publish(ctx context.Context, e port.Event) error {
	return k.proc.Process(ctx, kafka.Message{Key: []byte(e.Key), Value: e.Value, Time: time.Now()})
}
func (k *inProcessKafka) Close() error { return nil }

type platform struct {
	router *gin.Engine
	pub    *FakePublisher
}

var e2eSecret = []byte(strings.Repeat("e", 32))

func NewPlatform(t *testing.T) platform {
	t.Helper()
	gin.SetMode(gin.TestMode)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	// Processor side: real rules engine, idempotency guard and decision store.
	pub := &FakePublisher{}
	proc := &Processor{
		Eval:      rules.NewDefaultEngine(rdb, rules.DefaultFX(), rules.DefaultConfig()),
		Idemp:     NewIdempotency(rdb),
		Publisher: pub,
		Store:     NewRedisDecisionStore(rdb, time.Hour),
		Retry:     RetryPolicy{MaxAttempts: 2, Base: time.Millisecond, Max: time.Millisecond},
		Log:       log,
	}

	// Gateway side: real router, middleware chain, services and Redis adapter.
	st := store.NewRedisDecisions(rdb)
	allow, _ := middleware.NewIPAllowlist(nil)
	ipLim, clLim := ratelimit.NewLocal(1000, 1000), ratelimit.NewLocal(1000, 1000)
	t.Cleanup(ipLim.Close)
	t.Cleanup(clLim.Close)

	r := gin.New()
	_ = r.SetTrustedProxies(nil)
	routes.Register(r, routes.Deps{
		Log:           log,
		Allowlist:     allow,
		Auth:          middleware.NewAuthenticator(e2eSecret, "api-gateway", "", log),
		IPLimiter:     ipLim,
		ClientLimiter: clLim,
		Transactions:  handler.NewTransactionHandler(service.NewIngestionService(&inProcessKafka{proc: proc}, st, log), log),
		Decisions:     handler.NewDecisionHandler(service.NewDecisionService(st), log),
	})
	return platform{router: r, pub: pub}
}

func (p platform) call(t *testing.T, method, path, body, merchant string) *httptest.ResponseRecorder {
	t.Helper()
	tok, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, middleware.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: merchant, Issuer: "api-gateway", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		Scope:            "transactions:write transactions:read",
	}).SignedString(e2eSecret)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.RemoteAddr = "198.51.100.7:5000"
	req.Header.Set("Authorization", "Bearer "+tok)
	w := httptest.NewRecorder()
	p.router.ServeHTTP(w, req)
	return w
}

func txBody(id, user string, minor int64, country string) string {
	return `{"id":"` + id + `","user_id":"` + user + `","amount_minor":` + strconv.FormatInt(minor, 10) +
		`,"currency":"GBP","country":"` + country + `"}`
}

func TestFullLoopApprovedTransaction(t *testing.T) {
	p := NewPlatform(t)

	if w := p.call(t, "POST", "/v1/transactions", txBody("t-1", "u-1", 1999, "GB"), "merchant-a"); w.Code != 202 {
		t.Fatalf("submit: %d %s", w.Code, w.Body.String())
	}
	w := p.call(t, "GET", "/v1/transactions/t-1", "", "merchant-a")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"approved"`) {
		t.Fatalf("lookup: %d %s", w.Code, w.Body.String())
	}
	if d, _ := p.pub.counts(); d != 1 {
		t.Fatalf("exactly one decision event expected, got %d", d)
	}
}

// Two payments from the same user moments apart: the velocity rule reaches the
// quarantine threshold, and the merchant sees "under_review".
func TestFullLoopVelocityFraudIsQuarantined(t *testing.T) {
	p := NewPlatform(t)

	p.call(t, "POST", "/v1/transactions", txBody("t-1", "u-9", 500, "GB"), "merchant-a")
	p.call(t, "POST", "/v1/transactions", txBody("t-2", "u-9", 500, "GB"), "merchant-a")

	first := p.call(t, "GET", "/v1/transactions/t-1", "", "merchant-a")
	second := p.call(t, "GET", "/v1/transactions/t-2", "", "merchant-a")
	if !strings.Contains(first.Body.String(), `"approved"`) {
		t.Fatalf("first payment must pass: %s", first.Body.String())
	}
	if !strings.Contains(second.Body.String(), `"under_review"`) {
		t.Fatalf("rapid second payment must be quarantined: %s", second.Body.String())
	}
	if strings.Contains(second.Body.String(), "velocity") {
		t.Fatal("rule details must not leak to the merchant")
	}
}

func TestFullLoopTenantIsolation(t *testing.T) {
	p := NewPlatform(t)

	// Both merchants legitimately pick the same transaction id.
	if w := p.call(t, "POST", "/v1/transactions", txBody("order-1", "u-1", 1000, "GB"), "merchant-a"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := p.call(t, "POST", "/v1/transactions", txBody("order-1", "u-2", 1000, "GB"), "merchant-b"); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if d, _ := p.pub.counts(); d != 2 {
		t.Fatalf("merchant B's transaction must not be dropped as a duplicate of A's, got %d decisions", d)
	}
	if w := p.call(t, "GET", "/v1/transactions/order-1", "", "merchant-c"); w.Code != http.StatusNotFound {
		t.Fatalf("a third merchant must not see either, got %d", w.Code)
	}
}

func TestFullLoopResubmissionIsIdempotent(t *testing.T) {
	p := NewPlatform(t)
	for i := 0; i < 3; i++ { // a client retrying after a timeout
		if w := p.call(t, "POST", "/v1/transactions", txBody("t-1", "u-1", 1999, "GB"), "merchant-a"); w.Code != 202 {
			t.Fatalf("attempt %d: %d", i+1, w.Code)
		}
	}
	if d, _ := p.pub.counts(); d != 1 {
		t.Fatalf("three submissions of one id must yield one decision, got %d", d)
	}
}

func TestFullLoopInvalidInputNeverReachesTheProcessor(t *testing.T) {
	p := NewPlatform(t)
	if w := p.call(t, "POST", "/v1/transactions", txBody("t-1", "u-1", -5, "GB"), "m"); w.Code != 422 {
		t.Fatalf("got %d, want 422", w.Code)
	}
	if d, q := p.pub.counts(); d != 0 || q != 0 {
		t.Fatalf("decisions=%d dlq=%d: invalid input must be stopped at the gateway", d, q)
	}
}
