# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Ostrium Labs fork only: one image holding iggy-server, the iggy CLI and the Kafka
# gateway (iggy-gateway-kafka), so the two always move as one tested pair (Iggy's wire
# protocol is pre-1.0). Built natively per architecture by .github/workflows/loams-images.yml
# and joined into one multi-arch tag. Build context is the repository root:
#
#   docker build -f scripts/loams/Dockerfile .
#
# The default command runs iggy-server; the Loams compose `kafka` profile runs the gateway
# from the same image with `command: ["/usr/local/bin/iggy-gateway-kafka"]`.

ARG RUST_VERSION=1.99.1

FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-trixie AS chef
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    libhwloc-dev \
    libudev-dev \
    libssl-dev \
    pkg-config \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# The web UI iggy-server embeds (core/server/build.rs). Debian's own Node cannot load the
# TypeScript Vite config; the official image's Node can.
FROM node:24-trixie-slim AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build:static

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
# Dependencies first: this layer is keyed on recipe.json only, so a source change in the
# fork's own crates reuses it from the registry cache.
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    -p server -p iggy-cli -p iggy-gateway-kafka

COPY . .
COPY --from=web /web/build/static /app/web/build/static
COPY --from=web /web/node_modules /app/web/node_modules
# third-party-licenses.sh renders the web's npm licenses with Node, npm and npx.
COPY --from=web /usr/local/bin/node /usr/local/bin/node
COPY --from=web /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx
RUN cargo build --release --locked \
    --bin iggy-server --bin iggy --bin iggy-gateway-kafka && \
    cp target/release/iggy-server target/release/iggy target/release/iggy-gateway-kafka /app/

# Apache License 2.0 section 4: convenience binaries carry the license text of bundled code.
RUN TARGET="$(uname -m)-unknown-linux-musl" && \
    curl -sSfL --retry 5 --retry-all-errors \
      "https://github.com/EmbarkStudios/cargo-about/releases/download/0.9.0/cargo-about-0.9.0-${TARGET}.tar.gz" \
      | tar -xz -C /usr/local/bin --strip-components=1 "cargo-about-0.9.0-${TARGET}/cargo-about" && \
    ./scripts/ci/third-party-licenses.sh --generate \
      --manifest core/server/Cargo.toml \
      --manifest core/cli/Cargo.toml \
      --manifest gateways/kafka/Cargo.toml \
      --manifest web \
      --output /app/LICENSE-binary

FROM debian:trixie-slim AS runtime
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="iggy (Ostrium Labs fork with the Kafka gateway)" \
      org.opencontainers.image.description="iggy-server, the iggy CLI and iggy-gateway-kafka, built from the loams branch of ostrium-labs/iggy. Not an Apache Software Foundation release." \
      org.opencontainers.image.source="https://github.com/ostrium-labs/iggy" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${VCS_REF}"
WORKDIR /app
# netcat-openbsd: the gateway's healthcheck is a TCP probe.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libhwloc15 \
    libudev1 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/iggy-server /usr/local/bin/iggy-server
COPY --from=builder /app/iggy /usr/local/bin/iggy
COPY --from=builder /app/iggy-gateway-kafka /usr/local/bin/iggy-gateway-kafka
COPY --from=builder /app/LICENSE-binary /usr/share/doc/iggy/LICENSE-binary
COPY LICENSE NOTICE /usr/share/doc/iggy/

CMD ["/usr/local/bin/iggy-server"]
