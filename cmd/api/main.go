package main

import (
	"context"
	"errors"
	"log"
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
	app := server.New()

	// NotifyContext cancels ctx when the first signal arrives. Calling stop()
	// afterwards puts the default behaviour back, so a second Ctrl+C kills the
	// process at once instead of waiting politely for a request that is never
	// going to end.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Println("server starting on", app.HTTP.Addr)

		// ListenAndServe always returns a non-nil error. After Shutdown that
		// error is ErrServerClosed, which is the healthy path and not a crash,
		// so it must not be treated as one.
		if err := app.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Println("shutting down, press Ctrl+C again to force")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := app.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("shutdown failed: %v", err)
	}

	log.Println("server exited cleanly")
}
