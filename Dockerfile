# Multi-stage build: the toolchain that compiles the binary is thrown away, so
# the shipped image carries the binary and nothing else. It is the difference
# between an image around 20MB and one around 800MB.

FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded before the source. Docker caches each
# layer, so editing a .go file no longer invalidates the module download —
# only a change to go.mod or go.sum does.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary with no libc dependency, so it runs on
# a bare image. The ldflags strip the symbol table and DWARF debug info.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/api ./cmd/api


FROM alpine:3.22

# The API dials Postgres over TLS-capable connections and needs root
# certificates to validate them; the base image ships without any.
RUN apk add --no-cache ca-certificates

# Running as root inside a container is a real risk: a container escape starts
# from whatever user the process had. This one owns nothing and can log in
# nowhere.
RUN adduser -D -u 10001 appuser
USER appuser

COPY --from=build /out/api /usr/local/bin/api

EXPOSE 8080

CMD ["api"]
