# Simple Makefile for a Go project

# Build the application
all: build test

build:
	@echo "Building..."
	
	
	@go build -o main.exe cmd/api/main.go

# Run the application
run:
	@go run cmd/api/main.go

# Stage 6: the presence service, on its own.
#
# `make run` alone needs none of this — with no PRESENCE_ADDR the API answers
# presence from its own hub. Run both when you want the split locally:
#
#   make presenced                          # terminal 1, needs a Redis
#   PRESENCE_ADDR=localhost:9090 make run   # terminal 2
presenced:
	@go run ./cmd/presenced $(ARGS)

# The dev client: web/index.html on http://localhost:5173.
#
# It has to be served, not opened as a file. The API allows exactly one browser
# origin — http://localhost:5173 — for CORS and for the WebSocket handshake.
#
#   make run    # terminal 1: the API on :8080
#   make web    # terminal 2: this
#   make seed ARGS="-n 3"   # accounts to log in with
web:
	@go run ./cmd/web $(ARGS)
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

# Stage 5: prove that killing the broker delays messages and does not lose them.
#
# It sends while NATS is down. Every send must still answer 201, because the
# message and the instruction to deliver it are one transaction in Postgres.
# When NATS comes back the relay drains the backlog and everything arrives.
#
# Three numbers have to be right: MISSING 0, REFUSED 0, and the unread count of
# the user who never connected, which must equal the number of messages sent —
# not one more, which is what proves the consumer is idempotent.
#
#   docker compose up --build -d
#   make seed ARGS="-n 3"
#   make outboxcheck
#
# The real run: pause, and kill and restart NATS during it.
#
#   make outboxcheck ARGS="-pause 25s"
#   docker compose kill nats     # during the pause
#   docker compose start nats    # a few seconds later
outboxcheck:
	@go run ./cmd/outboxcheck $(ARGS)

# Stage 6: prove that killing the presence service costs one endpoint and
# nothing else — and watch the circuit breaker turn a slow failure into an
# instant one.
#
# Run it once plain, to see both nodes agree on who is online. Then run it with
# a pause and kill presenced during it. Two things to watch in the table: the
# presence column goes from ~1s failures to microsecond failures once the
# breaker opens, and the chat column never changes at all.
#
#   docker compose up --build -d
#   make seed ARGS="-n 2"
#   make presencecheck
#
#   make presencecheck ARGS="-pause 40s"
#   docker compose kill presenced      # during the pause
#   docker compose start presenced     # 15 seconds later
presencecheck:
	@go run ./cmd/presencecheck $(ARGS)

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

# Regenerate the gRPC code from proto/.
#
# The generated files are committed, so building and testing this repo needs
# none of this — run it only after editing a .proto.
#
# Everything here is a Go program, on purpose: buf is the protobuf compiler
# (protoc is a C++ binary that has to be installed and kept in step by hand),
# and the two plugins are what turn the .proto into Go. `go install` puts them
# in GOBIN, which has to be on PATH for buf to find them.
#
# `buf lint` runs first because a naming mistake is much easier to read here
# than in the generated code, and `buf breaking` is the one that matters once
# something is deployed: it compares against the committed version and fails on
# a change that would break a client which has not been rebuilt.
PROTOC_GEN_GO_VERSION ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
BUF_VERSION ?= v1.47.2

proto:
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) lint
	@go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate
	@go mod tidy

# Would this change break a client that has not been rebuilt? Compares the
# working tree against the last commit on main.
proto-breaking:
	@go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) breaking --against '.git#branch=main'

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

.PHONY: all build run presenced web seed wsload splitcheck gapcheck outboxcheck presencecheck \
	clean watch docker-run docker-down proto proto-breaking \
	test test-v test-one test-race itest cover \
	migrate-status migrate-up migrate-down migration
