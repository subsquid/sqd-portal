FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:latest-rust-1.83-slim-bookworm AS chef
WORKDIR /app

FROM --platform=$BUILDPLATFORM chef AS planner

COPY Cargo.toml .
COPY Cargo.lock .
COPY src ./src

RUN cargo chef prepare --recipe-path recipe.json


FROM --platform=$BUILDPLATFORM chef AS builder

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install protobuf-compiler pkg-config libssl-dev build-essential clang

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.toml .
COPY Cargo.lock .
COPY src ./src

RUN cargo build --release --workspace


FROM --platform=$BUILDPLATFORM debian:bookworm-slim
ARG TARGETOS
ARG TARGETARCH
ARG YQ_VERSION="4.40.5"

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
    && apt-get update \
    && apt-get -y install curl ca-certificates net-tools

RUN curl -sL https://github.com/mikefarah/yq/releases/download/v${YQ_VERSION}/yq_${TARGETOS}_${TARGETARCH} -o /usr/bin/yq \
    && chmod +x /usr/bin/yq

WORKDIR /run

COPY --from=builder /app/target/release/sqd-portal /usr/local/bin/sqd-portal

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"
ENV HTTP_LISTEN_ADDR="0.0.0.0:8000"

ENTRYPOINT ["sqd-portal"]

COPY healthcheck.sh .
RUN chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh
