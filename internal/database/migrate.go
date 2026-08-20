package database

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// migrationFS carries the .sql files into the compiled binary, so a built
// server sets up its own database with no extra files sitting next to it.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

// newProvider builds the goose runner over the embedded migrations.
//
// The session locker takes a Postgres advisory lock for the length of the run.
// Without it, two servers starting at the same moment would both see the same
// pending migration and both try to apply it. One would fail, and it would
// fail halfway.
func newProvider(db *service) (*goose.Provider, error) {
	sqlDB, err := db.db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql db: %w", err)
	}

	// The provider expects the migrations at the root of the filesystem it is
	// given, and the embed keeps them under migrations/.
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("open migrations dir: %w", err)
	}

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("build migration locker: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, sub,
		goose.WithSessionLocker(locker))
	if err != nil {
		return nil, fmt.Errorf("build migration provider: %w", err)
	}
	return provider, nil
}

// NewMigrator builds the goose runner over the embedded migrations, so the
// command in cmd/migrate can apply and inspect them without starting the API.
// The server itself never needs this: it calls Migrate at startup.
func NewMigrator() (*goose.Provider, error) {
	svc, ok := New().(*service)
	if !ok {
		return nil, fmt.Errorf("database.New() did not return the Postgres service")
	}
	return newProvider(svc)
}

// Migrate applies every migration that has not run yet.
//
// goose keeps a goose_db_version table and writes down each file it applies,
// so a migration runs exactly once and in order. Starting the server twice
// changes nothing the second time. This is the difference from AutoMigrate,
// which had no memory and guessed the shape of the database on every start.
//
// It pings first, so a bad connection string fails here with a clear error
// rather than on the first request.
func (s *service) Migrate(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("get sql db: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	provider, err := newProvider(s)
	if err != nil {
		return err
	}

	results, err := provider.Up(ctx)
	if err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	for _, r := range results {
		log.Printf("migration applied: %s (%s)", r.Source.Path, r.Duration)
	}
	return nil
}
