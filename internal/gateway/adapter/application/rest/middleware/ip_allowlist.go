package middleware

import (
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

// IPAllowlist restricts callers to known IPs or CIDR ranges. The list is an
// immutable snapshot swapped atomically, so request handling never contends
// with updates, and an Update that fails validation cannot half-apply.
type IPAllowlist struct {
	snap atomic.Pointer[allowSnapshot]
}

type allowSnapshot struct {
	all      bool
	prefixes []netip.Prefix
}

// NewIPAllowlist accepts exact IPs ("203.0.113.7"), CIDRs ("10.0.0.0/8"), or
// an empty list / "*" meaning allow everyone.
func NewIPAllowlist(entries []string) (*IPAllowlist, error) {
	a := &IPAllowlist{}
	if err := a.Update(entries); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *IPAllowlist) Update(entries []string) error {
	snap := &allowSnapshot{}
	if len(entries) == 0 {
		snap.all = true
	}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "*" {
			snap.all = true
			continue
		}
		if strings.Contains(e, "/") {
			p, err := netip.ParsePrefix(e)
			if err != nil {
				return fmt.Errorf("invalid CIDR %q: %w", e, err)
			}
			snap.prefixes = append(snap.prefixes, p.Masked())
			continue
		}
		ip, err := netip.ParseAddr(e)
		if err != nil {
			return fmt.Errorf("invalid IP %q: %w", e, err)
		}
		snap.prefixes = append(snap.prefixes, netip.PrefixFrom(ip, ip.BitLen()))
	}
	a.snap.Store(snap) // validated first, then swapped: never half-applied
	return nil
}

func (a *IPAllowlist) AllowsAll() bool { return a.snap.Load().all }

func (a *IPAllowlist) Allowed(ip string) bool {
	s := a.snap.Load()
	if s.all {
		return true
	}
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	addr = addr.Unmap() // treat ::ffff:1.2.3.4 as 1.2.3.4
	for _, p := range s.prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

func (a *IPAllowlist) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !a.Allowed(c.ClientIP()) {
			c.AbortWithStatusJSON(http.StatusForbidden, ErrorBody(c, "forbidden"))
			return
		}
		c.Next()
	}
}
