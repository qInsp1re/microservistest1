# ---------- Dockerfile ----------
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o counter .

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/counter .
ENV PORT=":3000"
CMD ["./counter"]
