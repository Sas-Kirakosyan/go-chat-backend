# Simple Makefile for a Go project

# Build the application
all: build test

build:
	@echo "Building..."
	
	
	@go build -o main.exe cmd/api/main.go

# Run the application
run:
	@go run cmd/api/main.go
# Seed the database with test users (override with e.g. `make seed ARGS="-n 20"`)
seed:
	@go run cmd/seed/main.go $(ARGS)

# Load test the WebSocket hub against a running server. Seed first: it logs in
# as the users cmd/seed created.
#
#   make seed ARGS="-n 5000"
#   make wsload ARGS="-n 5000 -messages 20"
#   make wsload ARGS="-n 100 -slow 5 -size 4000 -messages 600"   # slow clients
wsload:
	@go run ./cmd/wsload $(ARGS)

# Stage 3: prove whether two API nodes share their fan-out.
#
# It puts two members of one room on two different nodes and sends a message.
# Before Redis Pub/Sub only the sender's node delivers, and the tool says so.
#
#   docker compose up --build -d
#   make seed ARGS="-n 2"
#   make splitcheck
splitcheck:
	@go run ./cmd/splitcheck $(ARGS)

# Stage 4: prove that a client which loses its socket does not lose messages.
#
# It breaks a socket on purpose while messages keep being sent, reconnects, and
# repairs the hole with ?after_seq=. The number that matters is MISSING, and it
# has to be 0.
#
#   docker compose up --build -d
#   make seed ARGS="-n 2"
#   make gapcheck
#
# The harder run: pause, kill Redis during it, and watch live push die while
# the gap read still brings everything back.
#
#   make gapcheck ARGS="-pause 20s"
gapcheck:
	@go run ./cmd/gapcheck $(ARGS)

# Migrations. The server applies them itself on startup; these are for looking
# before you leap, and for stepping back after a mistake.
migrate-status:
	@go run ./cmd/migrate status

migrate-up:
	@go run ./cmd/migrate up

# Undoes the newest migration only, one step at a time. A down migration can
# lose data, so it is never "all the way back" in one command.
migrate-down:
	@go run ./cmd/migrate down

# Scaffold a new migration: make migration NAME=add_read_receipts
#
# -s keeps the numbering sequential (00003_, 00004_, ...) to match the files
# already here. Without it goose names the file after the clock instead, and
# the folder ends up in two different styles.
#
# The version pin matches the goose in go.mod, so the CLI and the server always
# read the files the same way.
migration:
	@go run github.com/pressly/goose/v3/cmd/goose@v3.26.0 -s \
		-dir internal/database/migrations create $(NAME) sql

# Create DB container
docker-run:
	@docker compose up --build

# Shutdown DB container
docker-down:
	@docker compose down

# ---------------------------------------------------------------------------
# Tests
#
# -count=1 is on every target on purpose. Go caches test results, so a second
# run prints "(cached)" and tests nothing. That cache is helpful in a big CI
# job and misleading on a laptop, where you re-run a test exactly because you
# just changed something.
#
# Docker must be running for the database tests: they start a real Postgres
# with testcontainers. Without it they fail with "cannot connect to the Docker
# API", which is the environment talking, not your code.
# ---------------------------------------------------------------------------

# Everything, quietly. One line per package. This is the one to use.
test:
	@echo "Testing..."
	@go test -count=1 ./...

# Everything, loudly: every test name and everything the server logged.
# Useful when a test fails and you want to see why.
test-v:
	@go test -count=1 -v ./...

# One test, or a group. The name is a regular expression:
#
#   make test-one NAME=TestSocketClosesWhenItsTokenExpires
#   make test-one NAME=TestRateLimit                        # every test starting with it
#   make test-one NAME=TestBucket PKG=./internal/server     # and only in one package
#
# Add PKG when you know where the test lives. Without it every package is
# opened, and internal/database starts a Postgres container before finding it
# has nothing to run — three wasted seconds on every attempt.
PKG ?= ./...
test-one:
	@go test -count=1 -v -run "$(NAME)" $(PKG)

# The database tests only. They are the slow ones, and the ones that need Docker.
itest:
	@echo "Running integration tests..."
	@go test -count=1 -v ./internal/database

# The race detector: it finds two goroutines touching the same memory at once.
# This project runs two goroutines per socket, so it is the most valuable test
# command here — and the one most likely to fail to start.
#
# It needs cgo and a C compiler. Without gcc on PATH it stops with
# "-race requires cgo". On Windows:
#
#   winget install BrechtSanders.WinLibs.POSIX.UCRT
#
# winget does not add it to PATH. See the README for the one-line fix, and note
# that VS Code must be fully restarted, not just given a new terminal tab.
test-race:
	@CGO_ENABLED=1 go test -count=1 -race ./...

# Which lines the tests actually run. Opens a coloured report in the browser:
# green is covered, red is not.
cover:
	@go test -count=1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

# Clean the binary
clean:
	@echo "Cleaning..."
	@rm -f main

# Live Reload
watch:
	@powershell -ExecutionPolicy Bypass -Command "if (Get-Command air -ErrorAction SilentlyContinue) { \
		air; \
		Write-Output 'Watching...'; \
	} else { \
		Write-Output 'Installing air...'; \
		go install github.com/air-verse/air@latest; \
		air; \
		Write-Output 'Watching...'; \
	}"

.PHONY: all build run seed wsload splitcheck gapcheck clean watch docker-run docker-down \
	test test-v test-one test-race itest cover \
	migrate-status migrate-up migrate-down migration
