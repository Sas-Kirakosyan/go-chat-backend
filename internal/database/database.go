package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ErrUserExists is returned by CreateUser when the username is already taken.
var ErrUserExists = errors.New("user already exists")

// ErrUserNotFound is returned by GetUserByUsername when no such user exists.
var ErrUserNotFound = errors.New("user not found")

// Service represents a service that interacts with a database.
type Service interface {
	// Health returns a map of health status information.
	// The keys and values in the map are service-specific.
	Health() map[string]string

	// Migrate creates or updates the tables the application needs.
	Migrate(ctx context.Context) error

	// CreateUser stores a new user. It returns ErrUserExists if the username
	// is already taken.
	CreateUser(ctx context.Context, username, passwordHash string) (*User, error)

	// GetUserByUsername looks a user up by name. It returns ErrUserNotFound if
	// there is no such user.
	GetUserByUsername(ctx context.Context, username string) (*User, error)

	// Close terminates the database connection.
	// It returns an error if the connection cannot be closed.
	Close() error
}

type service struct {
	db *gorm.DB
}

var (
	database   = os.Getenv("BLUEPRINT_DB_DATABASE")
	password   = os.Getenv("BLUEPRINT_DB_PASSWORD")
	username   = os.Getenv("BLUEPRINT_DB_USERNAME")
	port       = os.Getenv("BLUEPRINT_DB_PORT")
	host       = os.Getenv("BLUEPRINT_DB_HOST")
	schema     = os.Getenv("BLUEPRINT_DB_SCHEMA")
	dbInstance *service
)

func New() Service {
	// Reuse Connection
	if dbInstance != nil {
		return dbInstance
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&search_path=%s", username, password, host, port, database, schema)

	db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{
		// Turn driver-specific errors (like Postgres' 23505 unique_violation)
		// into GORM's portable ones, so we can compare against
		// gorm.ErrDuplicatedKey instead of matching on a driver error code.
		TranslateError: true,
		Logger:         logger.Default.LogMode(logLevel()),
	})
	if err != nil {
		log.Fatal(err)
	}

	// GORM wraps database/sql, so the connection pool is still configured there.
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	dbInstance = &service{
		db: db,
	}
	return dbInstance
}

// logLevel echoes every SQL statement while developing locally, and stays
// quiet everywhere else.
func logLevel() logger.LogLevel {
	if os.Getenv("APP_ENV") == "local" {
		return logger.Info
	}
	return logger.Warn
}

// Migrate creates or updates the tables the application needs. It pings first
// so a bad connection string fails here with a clear error rather than on the
// first request.
func (s *service) Migrate(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("get sql db: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// AutoMigrate creates missing tables, columns and indexes. It never drops
	// or narrows anything, so it is safe to run on every start.
	if err := s.db.WithContext(ctx).AutoMigrate(&User{}, &Conversation{}, &Message{}); err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	return nil
}

// CreateUser stores a new user. It returns ErrUserExists if the username is
// already taken.
func (s *service) CreateUser(ctx context.Context, username, passwordHash string) (*User, error) {
	user := &User{Username: username, PasswordHash: passwordHash}

	err := s.db.WithContext(ctx).Create(user).Error
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return nil, ErrUserExists
	}
	if err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	return user, nil
}

// GetUserByUsername looks a user up by name. It returns ErrUserNotFound if
// there is no such user.
func (s *service) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	var user User

	err := s.db.WithContext(ctx).Where("username = ?", username).First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("select user: %w", err)
	}
	return &user, nil
}

// Health checks the health of the database connection by pinging the database.
// It returns a map with keys indicating various health statistics.
func (s *service) Health() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stats := make(map[string]string)

	sqlDB, err := s.db.DB()
	if err != nil {
		stats["status"] = "down"
		stats["error"] = fmt.Sprintf("db down: %v", err)
		log.Printf("db down: %v", err)
		return stats
	}

	// Ping the database
	if err := sqlDB.PingContext(ctx); err != nil {
		stats["status"] = "down"
		stats["error"] = fmt.Sprintf("db down: %v", err)
		log.Printf("db down: %v", err) // Report it, but keep the server running
		return stats
	}

	// Database is up, add more statistics
	stats["status"] = "up"
	stats["message"] = "It's healthy"

	// Get database stats (like open connections, in use, idle, etc.)
	dbStats := sqlDB.Stats()
	stats["open_connections"] = strconv.Itoa(dbStats.OpenConnections)
	stats["in_use"] = strconv.Itoa(dbStats.InUse)
	stats["idle"] = strconv.Itoa(dbStats.Idle)
	stats["wait_count"] = strconv.FormatInt(dbStats.WaitCount, 10)
	stats["wait_duration"] = dbStats.WaitDuration.String()
	stats["max_idle_closed"] = strconv.FormatInt(dbStats.MaxIdleClosed, 10)
	stats["max_lifetime_closed"] = strconv.FormatInt(dbStats.MaxLifetimeClosed, 10)

	// Evaluate stats to provide a health message
	if dbStats.OpenConnections > 40 { // Assuming 50 is the max for this example
		stats["message"] = "The database is experiencing heavy load."
	}

	if dbStats.WaitCount > 1000 {
		stats["message"] = "The database has a high number of wait events, indicating potential bottlenecks."
	}

	if dbStats.MaxIdleClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many idle connections are being closed, consider revising the connection pool settings."
	}

	if dbStats.MaxLifetimeClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many connections are being closed due to max lifetime, consider increasing max lifetime or revising the connection usage pattern."
	}

	return stats
}

// Close closes the database connection.
// It logs a message indicating the disconnection from the specific database.
// If the connection is successfully closed, it returns nil.
// If an error occurs while closing the connection, it returns the error.
func (s *service) Close() error {
	log.Printf("Disconnected from database: %s", database)
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
