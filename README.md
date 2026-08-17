# go-chat-backend

A Go HTTP backend for a chat application: JWT-authenticated accounts on top of
Postgres, with conversations and messages stored per user.

Built with [Gin](https://gin-gonic.com/), [GORM](https://gorm.io/) and Postgres.
CORS is configured for a frontend on `http://localhost:5173`.

## Status

Authentication is implemented and tested. The conversation and message tables
are defined and migrated, but the endpoints that read and write them are not
built yet.

## Endpoints

| Method | Path             | Auth   | Description                          |
| ------ | ---------------- | ------ | ------------------------------------ |
| `GET`  | `/health`        | no     | Database connectivity and pool stats |
| `POST` | `/register`      | no     | Create an account                    |
| `POST` | `/login`         | no     | Exchange credentials for a JWT       |
| `GET`  | `/auth/profile`  | bearer | The current user                     |

Tokens are HS256, valid for 15 minutes, and sent as `Authorization: Bearer <token>`.

## Configuration

Create a `.env` file in the project root (it is gitignored):

```
PORT=8080
APP_ENV=local
JWT_SECRET=<a long random string>

BLUEPRINT_DB_HOST=localhost
BLUEPRINT_DB_PORT=5432
BLUEPRINT_DB_DATABASE=chat
BLUEPRINT_DB_USERNAME=chat
BLUEPRINT_DB_PASSWORD=<password>
BLUEPRINT_DB_SCHEMA=public
```

`JWT_SECRET` is required — the server refuses to start without it. Setting
`APP_ENV=local` echoes every SQL statement to the log.

Tables are created automatically on startup via GORM's `AutoMigrate`.

## Getting started

```bash
make docker-run   # start Postgres
make run          # start the API
```

## MakeFile

Run build make command with tests
```bash
make all
```

Build the application
```bash
make build
```

Run the application
```bash
make run
```
Create DB container
```bash
make docker-run
```

Shutdown DB Container
```bash
make docker-down
```

DB Integrations Test:
```bash
make itest
```

Live reload the application:
```bash
make watch
```

Run the test suite:
```bash
make test
```

Clean up binary from the last build:
```bash
make clean
```

The database tests use [testcontainers](https://testcontainers.com/) and need a
running Docker daemon.
