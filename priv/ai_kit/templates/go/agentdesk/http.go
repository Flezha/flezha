package main

// HTTP is the gateway's answer to a request: a status and a body, or the
// session to stream.
type HTTP struct {
	Status int    `json:"status,omitempty"`
	Body   string `json:"body,omitempty"`
	Stream string `json:"stream,omitempty"`
}
