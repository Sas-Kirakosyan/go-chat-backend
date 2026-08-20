package database

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func mustStartPostgresContainer() (func(context.Context, ...testcontainers.TerminateOption) error, error) {
	var (
		dbName = "database"
		dbPwd  = "password"
		dbUser = "user"
	)

	dbContainer, err := postgres.Run(
		context.Background(),
		"postgres:latest",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPwd),
		// The official Postgres image logs "ready to accept connections" twice:
		// once for the temporary server initdb uses to create the database, and
		// again for the real one. Waiting for the second occurrence is what keeps
		// us from connecting to the throwaway server.
		//
		// The timeout is a ceiling, not a delay — the wait returns the moment the
		// second line appears. It has to cover initdb on a cold start, which is
		// far more than five seconds under the WSL2 backend.
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return nil, err
	}

	database = dbName
	password = dbPwd
	username = dbUser

	// godotenv's autoload resolves .env against the working directory, which
	// under `go test` is this package's folder rather than the project root.
	// Nothing is loaded here, so every value the connection string needs has to
	// be set explicitly — including the schema, which would otherwise be empty
	// and make the migrations fail with "no schema has been selected to create in".
	schema = "public"

	dbHost, err := dbContainer.Host(context.Background())
	if err != nil {
		return dbContainer.Terminate, err
	}

	dbPort, err := dbContainer.MappedPort(context.Background(), "5432/tcp")
	if err != nil {
		return dbContainer.Terminate, err
	}

	host = dbHost
	port = dbPort.Port()

	return dbContainer.Terminate, err
}

func TestMain(m *testing.M) {
	teardown, err := mustStartPostgresContainer()
	if err != nil {
		log.Fatalf("could not start postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("could not teardown postgres container: %v", err)
	}
}

func TestNew(t *testing.T) {
	srv := New()
	if srv == nil {
		t.Fatal("New() returned nil")
	}
}

func TestHealth(t *testing.T) {
	srv := New()

	stats := srv.Health()

	if stats["status"] != "up" {
		t.Fatalf("expected status to be up, got %s", stats["status"])
	}

	if _, ok := stats["error"]; ok {
		t.Fatalf("expected error not to be present")
	}

	if stats["message"] != "It's healthy" {
		t.Fatalf("expected message to be 'It's healthy', got %s", stats["message"])
	}
}

func TestMigrateAndUsers(t *testing.T) {
	srv := New()
	ctx := context.Background()

	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() returned %v", err)
	}
	// Migrate must be safe to run against an already-migrated database.
	if err := srv.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate() returned %v", err)
	}

	created, err := srv.CreateUser(ctx, "alice", "hashed-password")
	if err != nil {
		t.Fatalf("CreateUser() returned %v", err)
	}
	if created.ID == 0 || created.Username != "alice" || created.CreatedAt.IsZero() {
		t.Fatalf("CreateUser() returned an incomplete user: %+v", created)
	}

	if _, err := srv.CreateUser(ctx, "alice", "another-hash"); !errors.Is(err, ErrUserExists) {
		t.Fatalf("duplicate CreateUser() returned %v, want ErrUserExists", err)
	}

	got, err := srv.GetUserByUsername(ctx, "alice")
	if err != nil {
		t.Fatalf("GetUserByUsername() returned %v", err)
	}
	if got.ID != created.ID || got.PasswordHash != "hashed-password" {
		t.Fatalf("GetUserByUsername() = %+v, want %+v", got, created)
	}

	if _, err := srv.GetUserByUsername(ctx, "nobody"); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("GetUserByUsername(missing) returned %v, want ErrUserNotFound", err)
	}
}

func TestClose(t *testing.T) {
	srv := New()

	if srv.Close() != nil {
		t.Fatalf("expected Close() to return nil")
	}

	// New() caches one connection pool in dbInstance and hands the same one to
	// every caller, so closing it leaves that cached pool dead. Any test that
	// runs after this one would then fail with "sql: database is closed", and
	// which tests those are depends only on file names. Clearing the cache
	// makes the next New() open a fresh pool, so the order stops mattering.
	dbInstance = nil
}
