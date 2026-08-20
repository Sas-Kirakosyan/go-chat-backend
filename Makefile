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

# Test the application
test:
	@echo "Testing..."
	@go test ./... -v
# Integrations Tests for the application
itest:
	@echo "Running integration tests..."
	@go test ./internal/database -v -count=1

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

.PHONY: all build run seed test clean watch docker-run docker-down itest \
	migrate-status migrate-up migrate-down migration
