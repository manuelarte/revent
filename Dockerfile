# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder

WORKDIR /app

# Install only the build-time packages needed by this crate.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libprotobuf-dev \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto

# Build.rs includes only `proto` and `proto/third_party/googleapis` paths, so copy
# protobuf well-known types there to satisfy google/protobuf imports.
RUN mkdir -p proto/third_party/googleapis/google \
    && cp -R /usr/include/google/protobuf proto/third_party/googleapis/google/

COPY src ./src
COPY resources/migrations ./resources/migrations
COPY static ./static
COPY openapi.yml ./openapi.yml
COPY tests ./tests

# Build with a locked dependency graph for reproducible installs.
RUN cargo build --release --locked \
    # The binary expects a .env file at startup
    && : > /tmp/.env

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

WORKDIR /app

COPY --from=builder /app/target/release/revent /usr/local/bin/revent
COPY --from=builder /tmp/.env /app/.env

EXPOSE 10000 10001

# Keep runtime defaults explicit and non-sensitive.
ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/revent"]

