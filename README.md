# Web Dictionary

A utility for extracting the most common Javascript function bodies from the HTTP Archive in Google BigQuery and building a zstd compression dictionary from them.

## Setup

1. **Authentication:**
   Ensure you have configured Google Cloud default credentials, as the script queries BigQuery.
   ```bash
   gcloud auth application-default login
   ```

2. **Python Environment:**
   Set up a virtual environment and install dependencies.
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python3 -m pip install -r requirements.txt
   ```

## Usage

Simply run the CLI script within the virtual environment:
```bash
python generate_dictionary.py
```

### What it does:
- Discovers the latest date partition in the HTTP Archive's script chunks dataset (`httparchive.crawl_staging.script_chunks`).
- Identifies javascript body hashes that appear on at least 10,000 unique URLs.
- Caches the script hashes to `cache/hashes.json` bound to its execution date.
- Extracts `script_chunks` payloads from BigQuery in batches (assuming BigQuery clustering by `hash` for efficiency) and caches them in `cache/content/`.
- Systematically tests compressing each chunk to see if a zstandard dictionary yields a > 50% size reduction over standard compression. Evaluates if the object adds sufficient delta context by applying a 64MB window size (level 11) to scan the entire cumulative dictionary.
- Filters incoming content to remove substrings (>= 50 bytes) that are already present in the active dictionary, minimizing duplicate content footprint.
- Accumulates a raw binary dictionary file (`data/dictionary.txt`) up to 40MB.
- Saves progress to `data/progress.json` allowing the script to be interrupted and resumed later safely.
