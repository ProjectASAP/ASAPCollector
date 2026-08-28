# User guide

This guide is command-oriented. Run commands from the repository root.

## Setup

```bash
./setup.sh
```

```bash
./setup.sh --no-go
```

## Build and test

```bash
./build_asap_otel.sh
```

```bash
go test ./...
```

## Distributed demonstration

```bash
bash deploy/mvp-multinode/scripts/run_demo.sh all
```

```bash
python3 -m unittest discover -s deploy/mvp-multinode/scripts/tests -p 'test_*.py'
```

See [distributed setup](otel-distributed-setup.md) and [production configuration](otel-production-distributed-config.md)
for deployment command sequences.

For readiness checks and symptom-based diagnosis, see
[operations and troubleshooting](operations-and-troubleshooting.md).
