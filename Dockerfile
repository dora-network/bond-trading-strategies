# syntax=docker/dockerfile:1.7

# ---- build stage ----
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=secret,id=github_token \
    TOKEN="$(cat /run/secrets/github_token)" && \
    test -n "$TOKEN" || (echo "github_token build secret is required" && exit 1) && \
    git config --global url."https://${TOKEN}:x-oauth-basic@github.com/".insteadOf "https://github.com/" && \
    go mod download && \
    rm -f /root/.gitconfig

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /mcp-server ./cmd/mcp-server
RUN CGO_ENABLED=0 go build -trimpath -o /strategy-server ./cmd/strategy-server
RUN CGO_ENABLED=0 go build -trimpath -o /price-daemon ./cmd/price-daemon

# ---- runtime stage ----
FROM alpine:3.21

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
COPY --from=builder /mcp-server /app/mcp-server
COPY --from=builder /strategy-server /app/strategy-server
COPY --from=builder /price-daemon /app/price-daemon

USER appuser

EXPOSE 8080
EXPOSE 8081

ENTRYPOINT ["/app/mcp-server"]
CMD ["-addr", ":8080", "-base-url", "http://localhost:8080"]
