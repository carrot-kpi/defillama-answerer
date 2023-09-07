FROM rust:bullseye AS base

WORKDIR /defillama-answerer
RUN cargo init
COPY Cargo.toml Cargo.toml
RUN cargo fetch
COPY src src
COPY abis abis
COPY migrations migrations
COPY build.rs build.rs
RUN cargo build --release --offline

FROM debian:bullseye-slim AS runner
RUN apt-get -y update
RUN apt-get -y install curl
RUN apt-get -y install postgresql
COPY --from=base /defillama-answerer/target/release/defillama-answerer /defillama-answerer

ARG CONFIG_PATH
ENV CONFIG_PATH=$CONFIG_PATH

ARG LOG_LEVEL
ENV LOG_LEVEL=$LOG_LEVEL

CMD ["/defillama-answerer"]
