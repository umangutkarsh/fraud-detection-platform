package middleware

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

const ctxScopes = "scopes"

// Claims is the token body. "scope" is a space-separated list, as in OAuth 2.
type Claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

// Authenticator validates HS256 bearer tokens. The secret comes from
// configuration; tokens must carry exp, sub and the expected issuer/audience;
// only HS256 is accepted; and responses never echo parser errors.
type Authenticator struct {
	secret   []byte
	issuer   string
	audience string
	log      *slog.Logger
	parser   *jwt.Parser
}

func NewAuthenticator(secret []byte, issuer, audience string, log *slog.Logger) *Authenticator {
	return &Authenticator{
		secret: secret, issuer: issuer, audience: audience, log: log,
		parser: jwt.NewParser(jwt.WithValidMethods([]string{"HS256"})),
	}
}

var errClaims = errors.New("missing or invalid claims")

func (a *Authenticator) Validate(raw string) (*Claims, error) {
	claims := &Claims{}
	tok, err := a.parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return a.secret, nil })
	if err != nil {
		return nil, err
	}
	if !tok.Valid || claims.Subject == "" || claims.ExpiresAt == nil { // exp is mandatory
		return nil, errClaims
	}
	if !claims.VerifyIssuer(a.issuer, true) {
		return nil, errClaims
	}
	if a.audience != "" && !claims.VerifyAudience(a.audience, true) {
		return nil, errClaims
	}
	if !claims.VerifyExpiresAt(time.Now(), true) {
		return nil, errClaims
	}
	return claims, nil
}

func (a *Authenticator) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if len(h) < 8 || !strings.EqualFold(h[:7], "bearer ") {
			c.Header("WWW-Authenticate", `Bearer realm="fraud-platform"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorBody(c, "unauthorized"))
			return
		}
		claims, err := a.Validate(strings.TrimSpace(h[7:]))
		if err != nil {
			a.log.Info("token rejected", "request_id", RequestIDFrom(c), "reason", err.Error())
			c.Header("WWW-Authenticate", `Bearer realm="fraud-platform", error="invalid_token"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorBody(c, "unauthorized"))
			return
		}
		scopes := map[string]struct{}{}
		for _, s := range strings.Fields(claims.Scope) {
			scopes[s] = struct{}{}
		}
		c.Set(ctxClientID, claims.Subject)
		c.Set(ctxScopes, scopes)
		c.Next()
	}
}

// RequireScope enforces least privilege per route: a token that may submit
// transactions need not be able to read decisions or reach other upstreams.
// 401 = who are you?  403 = you are known, but not allowed.
func RequireScope(scope string) gin.HandlerFunc {
	return func(c *gin.Context) {
		scopes, _ := c.Get(ctxScopes)
		if m, ok := scopes.(map[string]struct{}); ok {
			if _, has := m[scope]; has {
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusForbidden, ErrorBody(c, "forbidden: missing scope "+scope))
	}
}
