# Pulsy EKV

Clustered key-value store on [SlateDB](https://github.com/slatedb/slatedb) with gRPC API, NATS-based coordination, and pluggable storage backends (local disk, S3).

Supports namespaced key-value spaces, cross-node request forwarding, batch writes and OpenTelemetry observability.

## Quick Start

```bash
docker compose up -d
```

## Clients

```bash
dotnet add package Pulsy.EKV.Client
```

## Build & Test

```bash
dotnet build
dotnet test
```

## Docker

```bash
docker build -f Pulsy.EKV.Node/Dockerfile -t ekv-node .
```

Images: `ghcr.io/pulsy-global/ekv-node`

## License

[Apache-2.0](LICENSE)
