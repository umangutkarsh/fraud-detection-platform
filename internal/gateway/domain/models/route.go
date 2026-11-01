package models

import "net/http"

// ProxyRequest is everything the gateway needs to forward a call.
type ProxyRequest struct {
	Method    string
	Path      string
	RawQuery  string
	Header    http.Header
	Body      []byte
	ClientID  string // authenticated JWT subject
	ClientIP  string
	RequestID string
}

type ServiceResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}
