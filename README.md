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

## Key design decisions

- **Event-driven processing**  
  Claims and reverts are processed incrementally, event by event.  


- **Pure Python, no heavy frameworks**  
  Dictionaries and lightweight aggregates are sufficient for the required scale.  
  This keeps the solution simple and fast while leaving room to plug in Spark/Flink later if needed.

- **Clear separation of concerns**
  - Parsing & validation of input data
  - Core business logic and state
  - Output building and serialization

- **Future-ready**
  The same core can be reused for:
  - batch processing (current mode)
  - directory watching / streaming ingestion
  - alternative storage backends

---

## How to run

### Requirements
- Python 3.11+

No external dependencies are required.

### Run command

From the project root:

```bash
python -m events_processor.main \
  --pharmacies data_source/data/pharmacies \
  --claims data_source/data/claims \
  --reverts data_source/data/reverts \
  --out out
