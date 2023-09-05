<br />

<p align="center">
    <img src=".github/static/logo.svg" alt="Carrot logo" width="60%" />
</p>

<br />

<p align="center">
    Carrot is a web3 protocol trying to make incentivization easier and more capital
    efficient.
</p>

<br />

<p align="center">
    <img src="https://img.shields.io/badge/License-GPLv3-blue.svg" alt="License: GPL v3">
    <img src="https://github.com/carrot-kpi/defillama-answerer/actions/workflows/ci.yml/badge.svg" alt="CI">
</p>

# Carrot DefiLlama answerer

This package implements a daemon in Rust that handles the lifecycle of DefiLlama
oracles created in Carrot.

## Getting started

The package is developed using Rust, so in order to do anything it's necessary
to install the Rust toolchain on your machine.

In order to simply run the answerer in a sort of "dev" environment locally you
need a configuration file stored wherever you want. The configuration file's
format is the same as the `.config.example.yaml` file at the root of the repo.
Take that file, copy paste it, and rename it to `.config.yaml`, changing the
values that you want. By default, the data regarding contract addresses and
deployment blocks should be correct, so you'd only need to change the other
data.

A note on the IPFS API and Postgres connection. For convenience the repo
provides a Docker Compose configuration to quickly spin up a local Kubo (Go IPFS
client) and Postgres instance exposing the IPFS API at port 5001 and the
Postgres service at port 5432 of the host machine. If you decide to use the
provided configuration, simply run `docker compose up` to bootstrap the IPFS
node and the Postgres instance locally. The IPFS API endpoint to be used
provided you used this solution would be `http://127.0.0.1:5001`, while the
Postgres connections tring would be
`postgresql://user:password@127.0.0.1:5432/defillama-answerer`. These are the
default values of the provided `.config.example.yaml`.

Once the `.config.yaml` file is ready to be used and you've optionally
bootstrapped the IPFS node and Postgres instances through Docker Compose, and
assuming the file is named exactly `.config.yaml` and placed at the root of this
repo, you can just compile and start the daemon by running:

```
CONFIG_PATH="./.config.yaml" cargo run
```

If the config was set up correctly, at this point you should see the daemon
running smoothly.

An additional env variable `LOG_LEVEL` can be set to regulate which logs will be
shown. Its value can be one of `trace`, `debug`, `info`, `warn` or `error`.

## Testing with a local template playground

In most cases this program will be tested with a local Carrot template
playground. A few tips can be given for a good developer experience in this
case. In particular:

- Remember to start the playground before starting the answerer, as the answerer
  needs a local RPC URL, and the playground bootstraps a local node as part of
  its starting procedure.
- Remember to update your local file with the correct local RPC URL depending on
  the forked network on which the template is being set on.
- In cases where a lot of past blocks must be indexed from the local RPC, the
  local node's performance will be inevitably degraded, in some occasions even
  rendering it unusable in the playground when simulating transactions. In this
  case find out the current block number that the local node is pointing at
  (also printed in the program's startup logs) and set it in the `checkpoints`
  table in the database so that the program will detect the forked network to be
  completely synced. Having a forked chain with id `CHAIN_ID`, a block number
  `BLOCK_NUMBER` and the default connection string
  `postgresql://user:password@127.0.0.1:5432/defillama-answerer` you can easily
  update the checkpoint in the database by running
  `psql postgresql://user:password@127.0.0.1:5432/defillama-answerer` in your
  terminal followed by a
  `INSERT INTO checkpoints (chain_id, block_number) VALUES (<CHAIN_ID>, <BLOCK_NUMBER>) ON CONFLICT (chain_id) DO UPDATE SET block_number = <BLOCK_NUMBER>;`
  (remember the ending semicolon) in the Postgres prompt.
- Local nodes such as Ganache work by default in "automining" mode, meaning that
  no new block is produced unless a transaction is processed or unless manually
  triggered. This is a problem because the answerer reacts on new block events
  in order to process active oracles, so it might seem in certain cases that the
  answerer has stopped processing events while in reality it's just waiting for
  a new block to come. Luckily, most local nodes (such as Ganache and Anvil)
  support the `evm_mine` RPC method, so in order to trigger a new block on a
  local node running on a certain port `PORT` you can simply execute
  `curl -X POST --data '{ "method": "evm_mine", "params": [] }' http://localhost:<PORT>`
  in your terminal.

## Building a release binary

Building a release (i.e. optimized) binary is simple, just run:

```
cargo build --release
```

By default, the binary is placed under `/target/release/defillama-answerer` at
the root of the monorepo. You can run this program as a standalone binary
provided you have set the env variables described in the getting started
section.

## Building a Docker image

Building a Docker image is simple, just run:

```
docker build .
```

from the root of the repo
