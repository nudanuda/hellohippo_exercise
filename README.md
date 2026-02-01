The solution is implemented in *pure Python*, without heavy data-processing frameworks, and is designed to be efficient, readable, and extensible.

---

## What the application does

The application reads data from directories containing:
- **Pharmacies snapshot** (CSV)
- **Claims events** (JSON)
- **Reverts events** (JSON)

Based on these inputs, it produces three outputs:

### 1. Metrics by pharmacy and drug
For each `(npi, ndc)` pair:
- number of claims (`fills`)
- number of reverts
- average unit price (for active claims)
- total price (for active claims)

### 2. Top 2 cheapest chains per drug
For each `ndc`, the top 2 pharmacy chains with the lowest **average unit price per unit**.

### 3. Most common prescribed quantities per drug
For each `ndc`, the most frequently prescribed quantities.

---

## High-level architecture
```text
Input files (CSV / JSON)
        |
        v
Event parsing & validation
        |
        v
Event processor (claims / reverts)
        |
        v
In-memory state (aggregations & counters)
        |
        v
Output builders
        |
        v
JSON exports
```
---

## Key design decisions & trade-offs

- **Event-driven core**
Claims and reverts are processed as events, allowing both batch and streaming ingestion without changing business logic.

- **Explicit trade-offs**
 - All aggregations are kept in memory.
 - File discovery uses polling, not filesystem notifications.
 - JSON files are read fully (assumed to be reasonably sized).

- **Scalability path**
For very large datasets or high-volume streams, this design can be extended by:
	- replacing file polling with Kafka/PubSub/S3 events
	- moving aggregations to Spark Structured Streaming or Flink
	- persisting state in an external store

These changes would not affect the core event-processing logic

---
## Assumptions

- Input files are append-only
- Pharmacy snapshot is relatively stable (Pharmacy data is assumed to change infrequently and is fully reloaded at startup).
- JSON files are reasonably sized
- Event ordering is not guaranteed (Revert events may arrive before their corresponding claims and are handled accordingly).
---

## Requirements
- Python 3.11+
No external dependencies are required.

## How to run
The application supports two execution modes.

**1. Batch mode (default)**
Processes all existing files once and exits.
From the project root:
```bash
export PYTHONPATH=src
python -m events_processor.main \
  --pharmacies data_source/data/pharmacies \
  --claims data_source/data/claims \
  --reverts data_source/data/reverts \
  --out out
```
**2. Streaming mode (--streaming)**
Continuously watches directories for new files and processes them incrementally.

From the project root:
```bash
export PYTHONPATH=src
python -m events_processor.main \
  --pharmacies data_source/data/pharmacies \
  --claims data_source/data/claims \
  --reverts data_source/data/reverts \
  --out out \
  --streaming \
  --poll-interval 60
```

In streaming mode:

- the application does not terminate
- new JSON files are picked up on each poll
- outputs are updated incrementally

stop with Ctrl+C
