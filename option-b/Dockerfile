FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
# Note: confluent-kafka-go requires CGO and librdkafka.
# For production use golang:1.22-bullseye with apt-get install librdkafka-dev
RUN go mod download 2>/dev/null || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o game-server ./main.go

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/game-server .
EXPOSE 8080
ENTRYPOINT ["./game-server"]
