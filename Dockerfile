# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy application source code (including embedded web directory)
COPY . .

# Build self-contained static executable
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/server .

# Production minimal runtime
FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/server /app/server

# Render default port
ENV PORT=10000
EXPOSE 10000

CMD ["/app/server"]
