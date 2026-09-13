// Command seed fills the database with test users, and optionally with rooms
// that already have history in them. It goes through the same database.Service
// the API uses, so the rows it writes are identical to ones created by
// /register and by POST /conversations/:id/messages: bcrypt hash, GORM
// timestamps, unique index enforced, a real sequence number per message.
//
// Usage:
//
//	go run cmd/seed/main.go               # 100 users, testuser001..testuser100
//	go run cmd/seed/main.go -n 20         # 20 users
//	go run cmd/seed/main.go -prefix load  # load001, load002, ...
//
// Stage 8 added the rooms. A read load test needs history to page back
// through, and a fresh database has none:
//
//	go run cmd/seed/main.go -n 50 -rooms 20 -messages 200
//
// That is 20 rooms of 10 members with 200 messages each, which is four pages
// of scroll-back per room at the default page size of 50.
//
// Writing those messages goes through CreateMessage, so every one of them also
// writes an outbox row. The relay will publish all of them and the unread
// consumer will count all of them, which is a burst of background work that
// takes a little while to settle. That is on purpose: data seeded by a back
// door would not have the unread counters a real room has, and the load test
// reads those counters.
//
// Re-running is safe. Usernames that already exist are skipped, and so are
// rooms whose title is already there — the messages are not written twice.
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
	rooms := flag.Int("rooms", 0, "how many rooms to create, each with history; 0 creates none")
	roomSize := flag.Int("room-size", 10, "members per room")
	messages := flag.Int("messages", 200, "messages to write in each new room")
	roomPrefix := flag.String("room-prefix", "loadroom", "room title prefix; a zero-padded number is appended")
	flag.Parse()

	if *count < 1 {
		log.Fatal("-n must be at least 1")
	}
	if *rooms > 0 {
		// A room needs at least two people, or every message in it is written
		// by its only member and the unread counters stay at zero.
		if *roomSize < 2 {
			log.Fatal("-room-size must be at least 2")
		}
		if *roomSize > *count {
			log.Fatalf("-room-size %d needs at least that many users, but -n is %d", *roomSize, *count)
		}
		if *messages < 0 {
			log.Fatal("-messages cannot be negative")
		}
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

	// Users are fast. Rooms are not: every seeded message is its own
	// transaction, so the budget has to grow with how many were asked for.
	budget := 2 * time.Minute
	if *rooms > 0 {
		budget += time.Duration(*rooms**messages) * 50 * time.Millisecond
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
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

	if *rooms > 0 {
		seedRooms(ctx, db, roomConfig{
			count:       *rooms,
			size:        *roomSize,
			messages:    *messages,
			titlePrefix: *roomPrefix,
			userPrefix:  *prefix,
			userCount:   *count,
		})
	}
}

// roomConfig is what seedRooms needs, gathered in one place so the call site
// does not read as six anonymous arguments.
type roomConfig struct {
	count       int
	size        int
	messages    int
	titlePrefix string
	userPrefix  string
	userCount   int
}

// seedRooms creates rooms that already have history, so a read load test has
// something to page back through.
//
// Rooms rotate through the user list rather than all sharing the first few
// members. Otherwise every room would belong to users 1..size, the other
// seeded users would have an empty room list, and a load test spread over 50
// users would really be hammering ten of them.
func seedRooms(ctx context.Context, db database.Service, cfg roomConfig) {
	// Look the users up once. They all exist by now: the loop above either
	// created them or found them already there.
	users := make([]database.User, 0, cfg.userCount)
	for i := 1; i <= cfg.userCount; i++ {
		username := fmt.Sprintf("%s%03d", cfg.userPrefix, i)
		u, err := db.GetUserByUsername(ctx, username)
		if err != nil {
			log.Fatalf("look up %s: %v", username, err)
		}
		users = append(users, *u)
	}

	var madeRooms, skippedRooms, madeMessages int
	for i := 0; i < cfg.count; i++ {
		title := fmt.Sprintf("%s%03d", cfg.titlePrefix, i+1)

		// Members are a window over the user list that slides by one room and
		// wraps at the end.
		members := make([]database.User, 0, cfg.size)
		for j := 0; j < cfg.size; j++ {
			members = append(members, users[(i+j)%len(users)])
		}
		creator := members[0]

		// Idempotency without a new query: the creator's own room list already
		// carries every title they made.
		existing, err := db.ListConversationsForUser(ctx, creator.ID)
		if err != nil {
			log.Fatalf("list rooms of %s: %v", creator.Username, err)
		}
		if hasTitle(existing, title) {
			skippedRooms++
			continue
		}

		// members[0] is the creator, and CreateConversation adds them anyway.
		others := make([]uint, 0, len(members)-1)
		for _, m := range members[1:] {
			others = append(others, m.ID)
		}

		conv, err := db.CreateConversation(ctx, title, creator.ID, others)
		if err != nil {
			log.Fatalf("create %s: %v", title, err)
		}
		madeRooms++

		for n := 1; n <= cfg.messages; n++ {
			// Senders take turns, so no single user writes a whole room and
			// every member ends up with unread counts from the others.
			sender := members[n%len(members)]
			content := fmt.Sprintf("seeded message %d in %s", n, title)
			if _, _, err := db.CreateMessage(ctx, conv.ID, sender.ID, sender.Username, content, nil); err != nil {
				log.Fatalf("write message %d of %s: %v", n, title, err)
			}
			madeMessages++
		}
		log.Printf("  %s: %d members, %d messages", title, len(members), cfg.messages)
	}

	log.Printf("seeded %d rooms (%d already existed) and %d messages", madeRooms, skippedRooms, madeMessages)
	if madeMessages > 0 {
		log.Printf("the relay is now publishing %d outbox rows; give it a moment before measuring", madeMessages)
	}
}

func hasTitle(convs []database.Conversation, title string) bool {
	for _, c := range convs {
		if c.Title == title {
			return true
		}
	}
	return false
}
