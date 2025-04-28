FROM docker.io/rust:alpine3.20 AS rust-builder
ENV RUSTUP_HOME="/usr/local/rustup" \
    CARGO_HOME="/usr/local/cargo" \
    CARGO_TARGET_DIR="/tmp/target"
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static git protoc
WORKDIR /src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git <<"EOT" /bin/sh
    git clone https://github.com/serpent-os/tools /tools
    cd /tools
    cargo install --path ./boulder
EOT
ARG RUST_PROFILE="release"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/tmp/target \
    --mount=type=bind,target=/src <<"EOT" /bin/sh
    for target in vessel summit avalanche
    do
      cargo build -p "$target" --profile "$RUST_PROFILE"
      cp "/tmp/target/${RUST_PROFILE/dev/debug}/$target" /
    done
EOT

FROM docker.io/alpine:3.20 AS summit
WORKDIR /app
RUN apk add --no-cache sudo git curl
COPY --from=rust-builder /summit .
COPY ./crates/summit/static /app/static
VOLUME /app/state
VOLUME /app/config.toml
VOLUME /app/seed.toml
EXPOSE 5000
EXPOSE 5001
ENTRYPOINT ["/app/summit"]
CMD ["0.0.0.0", "--root", "/app", "--seed", "/app/seed.toml"]

FROM docker.io/alpine:3.20 AS vessel
WORKDIR /app
COPY --from=rust-builder /vessel .
VOLUME /app/state
VOLUME /app/config.toml
VOLUME /import
EXPOSE 5001
ENTRYPOINT ["/app/vessel"]
CMD ["0.0.0.0", "--root", "/app", "--import", "/import"]

FROM docker.io/alpine:3.20 AS avalanche
WORKDIR /app
RUN apk add --no-cache sudo git
COPY --from=rust-builder /avalanche .
COPY --from=rust-builder /usr/local/cargo/bin/boulder /usr/bin/boulder
COPY --from=rust-builder /tools/boulder/data/macros /usr/share/boulder/macros
VOLUME /app/state
VOLUME /app/config.toml
ENTRYPOINT ["/app/avalanche"]
CMD ["--root", "/app"]
