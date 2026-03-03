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
