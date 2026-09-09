# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

# Copy only the source needed by cmd/fillwire; no config or credential files
# are included in the image build context or final runtime layer.
COPY cmd ./cmd
COPY internal ./internal

RUN GOMAXPROCS=4 CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -p=4 -trimpath -ldflags='-s -w -buildid=' -o /out/fillwire ./cmd/fillwire

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/fillwire /usr/local/bin/fillwire

# No configuration is embedded. Mount a generic config and pass -config at run time.
ENTRYPOINT ["/usr/local/bin/fillwire"]
