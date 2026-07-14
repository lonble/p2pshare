# Multi-stage build: compile p2pshare + p2pc, then ship a small runtime image.
FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/p2pshare ./cmd/p2pshare
RUN CGO_ENABLED=0 go build -o /out/p2pc ./cmd/p2pc

FROM alpine:3.20
# curl+jq: used by entrypoint.sh to auto-bootstrap against a seed node.
RUN apk add --no-cache curl jq
COPY --from=build /out/p2pshare /app/p2pshare
COPY --from=build /out/p2pc /app/p2pc
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
