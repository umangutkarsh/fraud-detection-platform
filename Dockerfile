# syntax=docker/dockerfile:1
# One Dockerfile, two binaries:
#   docker build --build-arg APP=gateway   -t gateway:dev .
#   docker build --build-arg APP=processor -t processor:dev .

# Keep in step with the "go" line in go.mod.
ARG GO_VERSION=1.27.1

FROM golang:${GO_VERSION}-alpine AS build
ARG APP
WORKDIR /src

# git is needed for the "direct" fallback; ca-certificates for HTTPS.
RUN apk add --no-cache git ca-certificates

# Try the module proxy first; if it fails for any reason (including a
# timeout), fetch straight from the source repository.
ENV GOPROXY=https://proxy.golang.org|direct

# Copy only go.mod/go.sum first so the dependency download is cached and
# re-runs only when dependencies change, not on every code edit.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$APP" && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${APP}

# Distroless: no shell, no package manager, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]