package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"go-chat-backend/internal/metrics"
)

// requestIDHeader is both what we read from a proxy and what we send back, so
// a caller can quote the id from a failed call and we can find that exact
// request in the logs.
const requestIDHeader = "X-Request-Id"

// Keys for the values the middleware puts on the gin context.
const (
	requestIDKey = "requestID"
	loggerKey    = "logger"
)

// maxRequestIDLen caps an id we accept from outside. See requestID below for
// why an id from the network cannot be trusted as it arrives.
const maxRequestIDLen = 64

// The logger setup itself moved to internal/logging in Stage 6, because
// cmd/presenced needs the same one. What stays here is the middleware: the
// request id, the access log, and the panic recovery — all of them things only
// an HTTP server has.

// requestID gives every request an id and a logger that carries it.
//
// The point is joining lines together. One request writes a line when it
// arrives, maybe a warning in the middle, and a line when it finishes; without
// a shared id those are three unrelated rows in a log with a thousand other
// requests in it. With Stage 5 and 6 the same id will follow a message into a
// broker and into another service.
//
// An incoming id is reused, so a trace that started at the proxy is not cut in
// half here — but it is checked first. It comes from the network, it goes
// straight into a log line, and a caller who could put a newline or a megabyte
// in there would be writing our logs for us.
func (s *Server) requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(requestIDHeader)
		if !validRequestID(id) {
			id = newRequestID()
		}

		c.Set(requestIDKey, id)
		c.Set(loggerKey, slog.Default().With("request_id", id))
		c.Writer.Header().Set(requestIDHeader, id)

		c.Next()
	}
}

// validRequestID accepts only what is safe to print: a short string of
// letters, digits and the three separators an id normally uses.
func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return false
		}
	}
	return true
}

// newRequestID makes a 16-character random id.
//
// It does not have to be unique in the universe, only unique in a log you are
// reading, so eight random bytes is plenty and a UUID would only be longer.
// crypto/rand.Read never fails on any supported OS, and the error is ignored
// on purpose: a request must not fail because an id could not be made.
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// logFrom returns the logger for this request, the one carrying its id.
//
// It never returns nil. A handler called outside the middleware — a test, a
// route added later in the wrong group — gets the default logger instead of a
// panic.
func logFrom(c *gin.Context) *slog.Logger {
	if v, ok := c.Get(loggerKey); ok {
		if l, ok := v.(*slog.Logger); ok {
			return l
		}
	}
	return slog.Default()
}

// quietWhenHealthy are the routes that are asked over and over by machines: a
// Prometheus scrape every 15 seconds, a Kubernetes probe every few seconds.
// Logging those drowns out the requests a person cares about.
//
// They are only quiet while they are ANSWERING WELL. A probe that starts
// failing is one of the most interesting lines in the log, so anything from
// 400 up is written like any other request.
var quietWhenHealthy = map[string]bool{
	"/metrics": true,
	"/livez":   true,
	"/readyz":  true,
}

// requestLogger writes one line per finished request.
//
// # The query string is never logged
//
// Only URL.Path is written, never RawQuery. That is not tidiness, it is the
// reason /ws can be logged at all: a browser cannot set a header on a
// WebSocket handshake, so the access token travels as ?token=... Gin's own
// logger prints path and query together, which would write a live credential
// to disk on every connect. Do not add the query string back.
func (s *Server) requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		status := c.Writer.Status()
		if quietWhenHealthy[c.FullPath()] && status < http.StatusBadRequest {
			return
		}

		attrs := []any{
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", status,
			"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
			"bytes", c.Writer.Size(),
			"ip", c.ClientIP(),
		}
		// Who it was, when we know. It is set by AuthMiddleware, so an
		// unauthenticated route simply has no user_id on its lines.
		if userID, _ := currentUser(c); userID != 0 {
			attrs = append(attrs, "user_id", userID)
		}
		// Whatever the handler wrote into c.Error(...), which is where gin
		// collects errors that did not stop the response.
		if len(c.Errors) > 0 {
			attrs = append(attrs, "errors", c.Errors.String())
		}

		log := logFrom(c)
		switch {
		case status >= http.StatusInternalServerError:
			log.Error("request failed", attrs...)
		case status >= http.StatusBadRequest:
			log.Warn("request rejected", attrs...)
		default:
			log.Info("request", attrs...)
		}
	}
}

// recovery keeps a panic in one handler from killing the process.
//
// A panic in a goroutine that serves a request would otherwise take the whole
// server down, and with it every other request in flight and every open
// socket. One nil map in one rare branch must not be able to do that.
//
// The stack is logged, not returned. A stack trace in a response body tells a
// stranger the file layout and library versions of the server.
func (s *Server) recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			err := recover()
			if err == nil {
				return
			}

			log := logFrom(c).With(
				"panic", err,
				"method", c.Request.Method,
				"path", c.Request.URL.Path,
			)

			// A broken pipe is the client hanging up mid-response, not a bug in
			// us. The connection is already gone, so writing a 500 into it
			// would only panic a second time.
			if isBrokenPipe(err) {
				log.Warn("client went away mid-response")
				c.Abort()
				return
			}

			metrics.Panics.Inc()
			log.Error("panic recovered", "stack", string(debug.Stack()))
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		}()

		c.Next()
	}
}

// isBrokenPipe reports whether a panic value is really "the client hung up".
//
// Writing to a closed connection panics inside net/http, and that panic
// arrives here looking like any other. The string check is the fallback for
// wrapped errors that do not carry a syscall error any more.
func isBrokenPipe(err any) bool {
	netErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}

	if errors.Is(netErr, syscall.EPIPE) || errors.Is(netErr, syscall.ECONNRESET) {
		return true
	}

	msg := strings.ToLower(netErr.Error())
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") ||
		// What Windows says instead.
		strings.Contains(msg, "forcibly closed")
}
