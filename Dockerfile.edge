# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.24-bookworm AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# modernc.org/sqlite is pure Go, so these binaries can remain fully static.
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w -buildid=' -o /out/kis-mock-read ./cmd/kis-mock-read && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w -buildid=' -o /out/kis-mock-edge ./cmd/kis-mock-edge && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w -buildid=' -o /out/edge-canary ./cmd/edge-canary && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags='-s -w -buildid=' -o /out/gatewayd ./cmd/gatewayd

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/kis-mock-read /usr/local/bin/kis-mock-read
COPY --from=build /out/kis-mock-edge /usr/local/bin/kis-mock-edge
COPY --from=build /out/edge-canary /usr/local/bin/edge-canary
COPY --from=build /out/gatewayd /usr/local/bin/gatewayd

# Keep ENTRYPOINT unset so the selected binary is simply the docker run command.
CMD ["kis-mock-edge"]
