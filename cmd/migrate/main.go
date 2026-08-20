// Command migrate runs the database migrations without starting the API.
//
// The server already migrates itself on startup, so this command is for the
// times you want to look before you leap, or step back after a mistake:
//
//	go run ./cmd/migrate status    # what has run and what is still pending
//	go run ./cmd/migrate version   # the version the database sits at
//	go run ./cmd/migrate up        # apply everything pending
//	go run ./cmd/migrate down      # undo the newest migration, one step
//
// It reads the same .env as the server, so it talks to the same database.
// To point it somewhere else, override the variables for one call:
//
//	BLUEPRINT_DB_PORT=5433 go run ./cmd/migrate status
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pressly/goose/v3"

	"go-chat-backend/internal/database"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: migrate [status|version|up|down]\n")
	}
	flag.Parse()

	command := flag.Arg(0)
	if command == "" {
		command = "status"
	}

	provider, err := database.NewMigrator()
	if err != nil {
		log.Fatalf("could not open the database: %v", err)
	}

	// Two minutes is plenty for these migrations and still short enough that a
	// lock held by another process fails instead of hanging forever.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch command {
	case "status":
		err = status(ctx, provider)
	case "version":
		err = version(ctx, provider)
	case "up":
		err = up(ctx, provider)
	case "down":
		err = down(ctx, provider)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatalf("%s: %v", command, err)
	}
}

func status(ctx context.Context, p *goose.Provider) error {
	rows, err := p.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%-36s %-9s %s\n", "MIGRATION", "STATE", "APPLIED AT")
	for _, r := range rows {
		applied := "-"
		if !r.AppliedAt.IsZero() {
			applied = r.AppliedAt.Format(time.RFC3339)
		}
		fmt.Printf("%-36s %-9s %s\n", r.Source.Path, r.State, applied)
	}
	return nil
}

func version(ctx context.Context, p *goose.Provider) error {
	v, err := p.GetDBVersion(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("database is at version %d\n", v)
	return nil
}

func up(ctx context.Context, p *goose.Provider) error {
	results, err := p.Up(ctx)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		fmt.Println("nothing to do, the database is already up to date")
		return nil
	}
	for _, r := range results {
		fmt.Printf("applied %s in %s\n", r.Source.Path, r.Duration)
	}
	return nil
}

// down undoes one migration. It is deliberately one step and not "all the way
// back": a down migration can lose data, so it should be a decision each time
// rather than one command that empties the database.
func down(ctx context.Context, p *goose.Provider) error {
	result, err := p.Down(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("rolled back %s in %s\n", result.Source.Path, result.Duration)
	return nil
}
