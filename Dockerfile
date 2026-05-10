FROM golang:1.25-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build both binaries
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/scheduler ./cmd/scheduler
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/worker ./cmd/worker

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
# Copy compiled binaries from builder
COPY --from=builder /app/bin/scheduler /app/scheduler
COPY --from=builder /app/bin/worker /app/worker

# Expose scheduler port
EXPOSE 8080
