# Dan-Query

This repository provides an optimized pipeline for preparing data and running the final query process for the Dan-Query challenge.

---

## Project Overview

This project includes two main phases:

1. **Prepare Phase** — processes raw input data and generates optimized intermediate data.
2. **Run Phase** — runs the final query pipeline on the processed data and produces results.

---

## Setup

### 1. Clone the Repository

### 2. Install Requirements

Install the necessary dependencies before running any scripts:

```bash
pip install -r optimized/requirements.txt
```

---

## 🧾 Input Format

Before running the scripts, place your query results in `optimized/inputs.py` under the same format as the provided baseline example.

Make sure your data directory follows the structure expected by the scripts.


## Prepare Phase


**Run:**

```bash
python3 optimized/prepare.py \
  --data-dir /Users/wuhaodong/Downloads/data \
  --out-dir optimized-data
```

**This process may take around 10 minutes to complete.**

---

## Run Phase

Once the preparation phase finishes, run the final query model:

```bash
python3 optimized/run.py \
  --data-dir optimized-data \
  --out-dir final-results
```

Results will be saved in the `final-results/` directory.

---

## Output

- `optimized-data/` → intermediate data files from the prepare phase
- `final-results/` → final output files after the run phase
