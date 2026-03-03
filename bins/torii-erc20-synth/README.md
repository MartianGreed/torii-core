# torii-erc20-synth

Deterministic end-to-end ERC20 ingestion workload for profiling extractor -> decode -> sink -> DB write.

## Quick start (local Postgres)

```bash
docker compose up -d postgres
cargo run -p torii-erc20-synth --release
```

Artifacts are written to `perf/runs/<timestamp>/`:

- `report.json`
- `report.md`
- `context.env`
- `flamegraph.svg` (when built with `--features profiling`)

## Useful flags

```bash
cargo run -p torii-erc20-synth --release -- \
  --block-count 200 \
  --tx-per-block 1000 \
  --approval-ratio-bps 2000 \
  --blocks-per-batch 1 \
  --db-url postgres://torii:torii@localhost:5432/torii
```

Disable schema reset for append-mode experiments:

```bash
cargo run -p torii-erc20-synth -- --reset-schema false
```

Disable activity index writes and dedup conflict checks for pure ingest throughput experiments:

```bash
cargo run -p torii-erc20-synth --release -- \
  --disable-activity-index \
  --disable-dedup \
  --blocks-per-batch 200
```

Parity-preserving high-ingest mode (keeps activity + dedup, reduces write amplification and durability costs for benchmarks):

```bash
cargo run -p torii-erc20-synth --release -- \
  --defer-secondary-indexes \
  --unlogged-tables \
  --pg-sync-commit-off \
  --disable-activity-constraints \
  --defer-dedup-constraints
```
