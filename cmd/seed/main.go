// Command seed fills the database with test users. It goes through the same
// database.Service the API uses, so the rows it writes are identical to ones
// created by /register: bcrypt hash, GORM timestamps, unique index enforced.
//
// Usage:
//
//	go run cmd/seed/main.go               # 100 users, testuser001..testuser100
//	go run cmd/seed/main.go -n 20         # 20 users
//	go run cmd/seed/main.go -prefix load  # load001, load002, ...
//
// Re-running is safe: usernames that already exist are skipped, not duplicated.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"golang.org/x/crypto/bcrypt"

	"go-chat-backend/internal/database"
)

func main() {
	count := flag.Int("n", 100, "how many users to create")
	prefix := flag.String("prefix", "testuser", "username prefix; a zero-padded number is appended")
	password := flag.String("password", "password123", "plaintext password shared by every seeded user")
	flag.Parse()

	if *count < 1 {
		log.Fatal("-n must be at least 1")
	}
	// The API rejects shorter passwords, so a seeded user with one could never
	// log in through /login.
	if len(*password) < 8 {
		log.Fatal("-password must be at least 8 characters, to match the API's binding rules")
	}

	// Hash once and reuse. bcrypt at DefaultCost takes ~60ms per call by
	// design, so hashing per user would cost seconds for no benefit here: every
	// seeded user has the same password anyway. Real registrations still get
	// their own salt via RegisterHandler.
	hash, err := bcrypt.GenerateFromPassword([]byte(*password), bcrypt.DefaultCost)
	if err != nil {
		log.Fatalf("hash password: %v", err)
	}

	db := database.New()
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := db.Migrate(ctx); err != nil {
		log.Fatalf("could not prepare database: %v", err)
	}

	var created, skipped int
	for i := 1; i <= *count; i++ {
		username := fmt.Sprintf("%s%03d", *prefix, i)

		_, err := db.CreateUser(ctx, username, string(hash))
		if errors.Is(err, database.ErrUserExists) {
			skipped++
			continue
		}
		if err != nil {
			log.Fatalf("create %s: %v", username, err)
		}
		created++
	}

	log.Printf("seeded %d users (%d already existed), password %q", created, skipped, *password)
}
