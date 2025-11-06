<h1 align="center"><a href="https://mercure.rocks"><img src="public/mercure.svg" alt="Mercure: Real-time Made Easy" title="Live Updates Made Easy"></a></h1>

_Protocol and Reference Implementation_

Mercure is a protocol for pushing data updates to web browsers and other HTTP clients in a convenient, fast, reliable, and battery-efficient way.
It is especially useful to publish async and real-time updates of resources served through web APIs, to reactive web and mobile apps.

[![Awesome](https://awesome.re/badge.svg)](docs/ecosystem/awesome.md)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/mercure)](https://artifacthub.io/packages/search?repo=mercure)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/dunglas/mercure)](https://pkg.go.dev/github.com/dunglas/mercure)
[![CI](https://github.com/dunglas/mercure/actions/workflows/ci.yml/badge.svg)](https://github.com/dunglas/mercure/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/dunglas/mercure/badge.svg?branch=master)](https://coveralls.io/github/dunglas/mercure?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/dunglas/mercure)](https://goreportcard.com/report/github.com/dunglas/mercure)

![Subscriptions Schema](spec/subscriptions.png)

- [Getting started](https://mercure.rocks/docs/getting-started)
- [Full documentation](https://mercure.rocks/docs)
- [Demo](https://demo.mercure.rocks/)

[The protocol](https://mercure.rocks/spec) is maintained in this repository and is also available as [an Internet-Draft](https://datatracker.ietf.org/doc/draft-dunglas-mercure/).

A reference, production-grade, implementation of [**a Mercure hub**](https://mercure.rocks/docs/hub/install) (the server) is also available in this repository.
It's free software (AGPL) written in Go. It is provided along with a library that can be used in any Go application to implement the Mercure protocol directly (without a hub) and [an official Docker image](https://hub.docker.com/r/dunglas/mercure).

In addition, a managed and high-scalability version of the Mercure.rocks hub is [available on Mercure.rocks](https://mercure.rocks/pricing).

## Development Quick Start

```bash
# Compile the binary
go build -o mercure .

# Run tests
go test -v ./...

# Build Docker image (requires compiled binary)
docker build -t odoucet/mercure-ha .
```

## Why this fork?

The original Mercure project does not include native High Availability support. This feature is only available through the [official SaaS offering](https://mercure.rocks/pricing) or via a [dedicated on-premise HA license](https://mercure.rocks/docs/hub/cluster#high-availability-on-premise-version), which costs several thousand dollars per year — but includes professional support.

This fork was created to experiment with and deploy an internal High Availability solution at [Oxeva](https://www.oxeva.fr).
It comes **without any warranty or support**, and is **intended for Oxeva internal or customer use only**.

The source code is published to comply with the terms of the **GNU Affero General Public License v3.0 (AGPL-3.0)**.
Use it at your own risk.

If you find a bug or improvement opportunity, feel free to open a pull request — it may be reviewed and merged (or not).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License and Copyright

See [license information](https://mercure.rocks/docs/hub/license).

## Credits

Created by [Kévin Dunglas](https://dunglas.fr). Graphic design by [Laury Sorriaux](https://github.com/ginifizz).
Sponsored by [Les-Tilleuls.coop](https://les-tilleuls.coop).
