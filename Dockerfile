# syntax=docker/dockerfile:1.7
# zoo-operator image. hanzo-operator-core is a PRIVATE git dependency
# (github.com/hanzoai/operator-core), so cargo needs a GitHub token to fetch it.
# Canonical pattern: a buildkit secret `gh_token`, injected via GIT_CONFIG_COUNT
# env (insteadOf rewrite) for the duration of the RUN only — the token never
# touches disk and is never committed to a layer. CARGO_NET_GIT_FETCH_WITH_CLI
# makes cargo shell out to git, which honours the env-supplied config.
FROM rust:1.88-bookworm AS builder
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
# Dependency-cache layer: build deps against a stub main so source edits don't
# re-fetch/re-compile the whole tree.
RUN --mount=type=secret,id=gh_token \
    GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0="url.https://x-access-token:$(cat /run/secrets/gh_token)@github.com/.insteadOf" \
    GIT_CONFIG_VALUE_0="https://github.com/" \
    sh -c 'mkdir src && echo "fn main(){}" > src/main.rs && cargo build --release && rm -rf src'
COPY src/ src/
RUN --mount=type=secret,id=gh_token \
    GIT_CONFIG_COUNT=1 \
    GIT_CONFIG_KEY_0="url.https://x-access-token:$(cat /run/secrets/gh_token)@github.com/.insteadOf" \
    GIT_CONFIG_VALUE_0="https://github.com/" \
    sh -c 'touch src/main.rs && cargo build --release'

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/zoo-operator /usr/local/bin/zoo-operator
ENTRYPOINT ["zoo-operator"]
