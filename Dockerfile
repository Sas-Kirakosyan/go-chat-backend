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
#
# Two binaries since Stage 6, from one build and into one image.
#
# One image and not two is a deliberate choice for a project this size: both
# come from the same module and the same go.sum, so a single image cannot drift
# into "the API was built from a commit the presence service was not". Compose
# picks which one runs with `command:`. The real reason to split the image
# later is deploy independence — shipping a presence fix without rebuilding the
# API — and that is a thing to do when the two are released on different days,
# not before.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/presenced ./cmd/presenced


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
COPY --from=build /out/presenced /usr/local/bin/presenced

# 8080 is the API. 9090 is the presence service's gRPC port and 9091 its
# metrics page; only the presenced service in compose ever listens on those.
EXPOSE 8080 9090 9091

CMD ["api"]
