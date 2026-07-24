# Multi-arch, static, distroless. Built with:
#   docker buildx build --platform linux/amd64,linux/arm64 -t kafka-wire .
ARG GO_VERSION=1.24
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=none

WORKDIR /src
# Dependencies first so a source-only change does not re-download the module
# graph. go.sum is committed, so builds are verified and reproducible.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/kafka-wire ./cmd/kafka-wire

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/kafka-wire /usr/local/bin/kafka-wire

# The image runs as an unprivileged user. Whatever you mount at /data must be
# writable by uid 65532, or the broker will exit on startup saying so. Some
# hosted platforms mount volumes root-owned; on those, override with
# --user 0:0 and accept the tradeoff knowingly.
USER nonroot:nonroot

ENV KAFKA_WIRE_STORAGE_DATADIR=/data \
    KAFKA_WIRE_LISTENERS_KAFKA=0.0.0.0:9092 \
    KAFKA_WIRE_LISTENERS_ADMIN=0.0.0.0:8080

# Binding 0.0.0.0 inside a container is normal, but the broker refuses to
# start on a non-loopback address without authentication unless you say you
# meant it. In a private container network that is what auth.allowanon is for.
EXPOSE 9092 8080
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/kafka-wire"]
CMD ["serve"]
