package main

import "regexp"

// Request is one HTTP request, as the plugin hands it to the gateway. The
// gateway knows two routes: POST /sessions/<user>/messages, whose body is the
// message, and GET /sessions/<user>/events, the session's answers as they are
// written. Fields the envelope carries beside these are ignored.
type Request struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Body   string `json:"body"`
}

// route is /sessions/<user>/<what>, the user a short lower-case name.
var route = regexp.MustCompile(`^/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)$`)

func (r Request) Sends() bool { return r.is("POST", "messages") }

func (r Request) Listens() bool { return r.is("GET", "events") }

func (r Request) User() string { return r.route(1) }

func (r Request) Message() Message { return Message{User: r.User(), Text: r.Body} }

func (r Request) is(verb, what string) bool { return r.Method == verb && r.route(2) == what }

// route is one part of the route, "" when the path is not a route.
func (r Request) route(part int) string {
	if match := route.FindStringSubmatch(r.Path); match != nil {
		return match[part]
	}
	return ""
}
