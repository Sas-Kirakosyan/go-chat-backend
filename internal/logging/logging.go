// Package logging installs the process-wide structured logger.
//
// It exists because Stage 6 made a second process. Until then this was a
// private function in internal/server, which was right while there was one
// binary: the logger was part of building the app. The moment cmd/presenced
// appeared, the choice was to copy fifteen lines or to move them, and copying
// them would have meant two services whose logs slowly stopped matching — one
// switches to JSON, the other does not, and a log shipper reading both gets
// half a stream it cannot parse.
//
// That is the small, boring version of the lesson the whole stage is about:
// splitting a service is mostly discovering what the two halves shared.
package logging

import (
	"log/slog"
	"os"
)

// Setup installs the default logger and returns it.
//
// Structured means every line is key/value pairs, not a sentence. "a request
// was slow" is a thing a human reads one of; status=500 route=/login is a
// thing a machine can count, filter and alert on.
//
// JSON in production, plain text locally: a log shipper wants JSON, and a
// person watching a terminal does not.
//
// slog.SetDefault also redirects the old log package, so any log.Printf left
// anywhere in the tree — or inside a dependency — comes out in the same format
// instead of bypassing all of this.
func Setup() *slog.Logger {
	opts := &slog.HandlerOptions{Level: Level()}

	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if os.Getenv("APP_ENV") == "local" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	logger := slog.New(handler)

	// NODE_ID says which process wrote the line. With one node it is noise;
	// with two API nodes and a presence service it is the first thing you need,
	// because "the socket never got the message" and "this node never had the
	// socket" look the same in a log that cannot tell the processes apart.
	if node := os.Getenv("NODE_ID"); node != "" {
		logger = logger.With("node", node)
	}

	slog.SetDefault(logger)
	return logger
}

// Level reads LOG_LEVEL.
func Level() slog.Level {
	var level slog.Level
	// UnmarshalText understands "debug", "info", "warn", "error", and any of
	// them with an offset like "warn+2". An unset or unreadable value leaves
	// the zero value, which is Info.
	if err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return level
}

// NodeID is how a process names itself in logs and to the other services.
// NODE_ID in compose, the hostname otherwise — inside a container that is the
// container id, which is exactly what you want to grep for.
func NodeID() string {
	if id := os.Getenv("NODE_ID"); id != "" {
		return id
	}
	if host, err := os.Hostname(); err == nil {
		return host
	}
	return "unknown"
}
