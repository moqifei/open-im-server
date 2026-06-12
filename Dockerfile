# Use Go 1.22 Alpine as the base image for building the application
FROM golang:1.25-alpine AS builder

# Define the base directory for the application as an environment variable
ENV SERVER_DIR=/openim-server

# Set the working directory inside the container based on the environment variable
WORKDIR $SERVER_DIR

# Set the Go proxy to improve dependency resolution speed
# ENV GOPROXY=https://goproxy.io,direct
ENV GOPROXY=https://goproxy.cn,direct

# Copy all files from the current directory into the container
COPY . .

# Pin gomake to v0.0.14 (≤v0.0.14 has old API: WithSpinner, PathOptions, Build(bin, nil, nil))
# v0.0.15-alpha.1+ removed these APIs; go.mod v0.0.17 also has new API.
RUN go get github.com/openimsdk/gomake@v0.0.14 && go mod download

# Install Mage to use for building the application
RUN go install github.com/magefile/mage@v1.15.0

# Optionally build your application if needed
RUN mage build

# Using Alpine Linux with Go environment for the final image
FROM golang:1.25-alpine

# Install necessary packages, such as bash
RUN apk add --no-cache bash

# Set the environment and work directory
ENV SERVER_DIR=/openim-server
WORKDIR $SERVER_DIR


# Copy the compiled binaries and mage from the builder image to the final image
COPY --from=builder $SERVER_DIR/_output $SERVER_DIR/_output
COPY --from=builder $SERVER_DIR/config $SERVER_DIR/config
COPY --from=builder /go/bin/mage /usr/local/bin/mage
# Copy Go module cache so final stage is truly offline
COPY --from=builder /go/pkg/mod /go/pkg/mod
COPY --from=builder $SERVER_DIR/magefile_windows.go $SERVER_DIR/
COPY --from=builder $SERVER_DIR/magefile_unix.go $SERVER_DIR/
COPY --from=builder $SERVER_DIR/magefile.go $SERVER_DIR/
COPY --from=builder $SERVER_DIR/start-config.yml $SERVER_DIR/
COPY --from=builder $SERVER_DIR/go.mod $SERVER_DIR/
COPY --from=builder $SERVER_DIR/go.sum $SERVER_DIR/
ENV GOPROXY=off

# Set the command to run when the container starts
ENTRYPOINT ["sh", "-c", "mage start && tail -f /dev/null"]