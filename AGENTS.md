# Web Dictionary: AI Agent Guidance & Context

Welcome. You are working on the **Web Dictionary** utility, a utility for extracting the most common javascript function bodies from the HTTP Archive and building a compression dictionary from them..

## 🎯 Directives for AI Agents

When working on this codebase, you must adhere to the following strict architectural principles and workflow:

1. **Mandatory Project Context & Workflow:**
   - **Initial Step:** At the beginning of any task, you must read `README.md` to understand the project purpose and usage.
   - **Closing Step:** At the end of each conversation, you must update `README.md` with any documentation or API changes, update `requirements.txt` with any changes to pip module requirements, and update `AGENTS.md` with any information that would benefit future agents either as a result of the current conversation or as discovered while working on the current conversation (it is the long-term memory for the project).

2. **Hygiene & Temporary Assets**: During active development workflows or parsing investigations, AI agents will frequently create throw-away diagnostic scripts (like `test.js` or `compress.py`), generated sample outputs (like `out.json`), or massive `.har` test outputs scattered in the root directory. **Agents MUST meticulously track and clean up** (delete) all standalone testing hooks, debugging `.log` files, and generic CLI scratch files prior to concluding their development sequence globally to prevent polluting the repository tree.

3. **Code Style & Quality:**: Python code should follow PEP 8 guidelines and Google's Python Style Guide.

4. **Google Cloud Authentication**:
   - **Project Context:** This project uses Google Cloud services (BigQuery). The default credentials and project should already be set.
   - **Verification:** Use `gcloud auth application-default print-access-token` to verify that authentication is working.

5. **Python venv**: The project uses a Python virtual environment. Use `source .venv/bin/activate` to activate it. You can verify that the virtual environment is activated by running `which python` and checking that it points to the virtual environment. If it is not activated, activate it and then run `pip install -r requirements.txt` to install the dependencies. If dependencies are added or removed, update `requirements.txt` and run `pip install -r requirements.txt` to install the dependencies. If the venv is not present, run `python -m venv .venv` to create it and then `pip install -r requirements.txt` to install the dependencies.

6. **Web Compression Dictionary Script**: The `generate_dictionary.py` CLI script runs queries on BigQuery (using a partition filter `WHERE date >= '2020-01-01'` to query the latest max date partition in `httparchive.crawl_staging.script_chunks` first) and compiles a common JS body dictionary.
   - It caches the query state to `cache/hashes.json` bound to the active parsed maximum partition date.
   - The fetch logic uses batched BigQuery queries `WHERE hash IN (...)`. This requires the BigQuery table to be clustered by `hash` to execute efficiently and avoid massive column scan costs!
   - Chunk bodies are cached persistently to `cache/content/`.
   - The compiled dictionary output logic is preserved incrementally within `data/dictionary.txt` and tracks script bounds with `data/progress.json`. Auto-resets if fully completed or limits size appropriately.
   - For python zst compression, it depends on the `zstandard` module available via pip. It applies a 64MB window (`window_log=26`) at level 11 to scan the entire cumulative dictionary.
   - The dictionary builder filters overlapping substrings >=50 bytes using a custom sliding-window hash index over `dictionary_bytes` for optimal O(N) deduplication speeds.

7. **Evaluating Dictionary Compression**: The `test_dictionary.py` CLI script validates the generated dictionary's effectiveness against real HTTP Archive crawl URLs.
   - Requires `requests`, `zstandard`, and `brotli` pip packages (brotli handles initial payload decompression).
   - Fetches requests from `httparchive.crawl.requests` matching the partition date, limited to `rank < 100000` for script/html types.
   - Operates in a highly parallel fashion using a thread pool with length 10, skipping already-processed URLs from `data/test_results.jsonl` safely using a synchronized write Queue.
   - Decompresses `gzip`, `br`, or `zstd` payloads over the wire to acquire the true uncompressed byte counts.
   - Evaluates dict efficiency using natively built zstandard (ZstdCompressor at level 21, window_log 26) applying a pre-parsed ZstdCompressionDict vs a base ZstdCompressor (level 21, window_log 26).
   - Offers an `--analyze` flag that evaluates percentage savings relative to standard Zstd-21 with detailed distribution percentile cut-points (25th, 50th, 75th, 90th, 95th, 99th).
