package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-chat-backend/internal/server"
)

// shutdownTimeout is how long a request that is already running gets to finish
// after the signal arrives.
//
// Kubernetes sends SIGKILL 30 seconds after SIGTERM by default, so this stays
// well under that: a shutdown that outlives the grace period is not a graceful
// shutdown, it is a kill with extra steps.
const shutdownTimeout = 10 * time.Second

func main() {
	// server.New installs the structured logger before it does anything else,
	// so every slog call below writes in the same format as the rest of the
	// service.
	app := server.New()

	// NotifyContext cancels ctx when the first signal arrives. Calling stop()
	// afterwards puts the default behaviour back, so a second Ctrl+C kills the
	// process at once instead of waiting politely for a request that is never
	// going to end.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("server starting", "addr", app.HTTP.Addr)

		// ListenAndServe always returns a non-nil error. After Shutdown that
		// error is ErrServerClosed, which is the healthy path and not a crash,
		// so it must not be treated as one.
		if err := app.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	stop()
	slog.Info("shutting down, press Ctrl+C again to force")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown failed", "err", err)
		os.Exit(1)
	}

	slog.Info("server exited cleanly")
}
