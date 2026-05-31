FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/erp-mock ./cmd/erp-mock

FROM scratch

USER 65532:65532

COPY --from=builder /out/erp-mock /erp-mock

EXPOSE 8080

ENTRYPOINT ["/erp-mock"]
