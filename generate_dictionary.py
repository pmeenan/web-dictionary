#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys

# Disable mTLS to avoid required OpenSSL dependencies on workstations
os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"

from google.cloud import bigquery
import zstandard

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CACHE_DIR = "cache"
CONTENT_CACHE_DIR = os.path.join(CACHE_DIR, "content")
HASHES_CACHE_FILE = os.path.join(CACHE_DIR, "hashes.json")

DATA_DIR = "data"
DICT_FILE = os.path.join(DATA_DIR, "dictionary.txt")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")

MAX_DICT_SIZE = 40 * 1024 * 1024  # 40MB

def init_directories():
    os.makedirs(CONTENT_CACHE_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

def get_max_date(client):
    """
    Queries BigQuery to find the most recent partition date in the script_chunks dataset.
    This ensures we are always computing our dictionary against the latest crawl.
    """
    logging.info("Querying max date from httparchive.crawl_staging.script_chunks...")
    query = "SELECT MAX(date) as max_date FROM `httparchive.crawl_staging.script_chunks` WHERE date >= '2020-01-01'"
    job = client.query(query)
    results = list(job.result())
    if not results or not results[0].max_date:
        logging.error("Could not find a max date.")
        sys.exit(1)
    max_date = results[0].max_date
    logging.info(f"Using max date: {max_date}")
    return max_date

def get_hashes(client, max_date):
    """
    Retrieves a prioritized list of script chunk hashes for the given date.
    
    1. Checks the local cache first to avoid re-querying expensive aggregations.
    2. Validates the cache's creation date against the active max_date.
    3. If necessary, queries BigQuery to find script hashes that appear on >= 10,000 unique URLs.
    """
    if os.path.exists(HASHES_CACHE_FILE):
        logging.info("Loading cached hashes...")
        with open(HASHES_CACHE_FILE, "r") as f:
            cached_data = json.load(f)
            if isinstance(cached_data, dict) and cached_data.get("date") == str(max_date):
                return cached_data["hashes"]
            elif isinstance(cached_data, list):
                logging.info("Cached hashes are in old format. Will re-query.")
            else:
                logging.info(f"Cached hashes date does not match {max_date}. Will re-query.")

    logging.info(f"Querying script hashes for date {max_date}...")
    # Get hash and count of DISTINCT urls where count >= 10000.
    query = f"""
    SELECT
      `hash`,
      COUNT(DISTINCT url) AS url_count,
      ANY_VALUE(url) AS sample_url
    FROM
      `httparchive.crawl_staging.script_chunks`
    WHERE
      date = '{max_date}'
    GROUP BY
      `hash`
    HAVING
      url_count >= 10000
    ORDER BY
      url_count DESC
    """
    job = client.query(query)
    hashes = []
    for row in job:
        hashes.append({
            "hash": row.hash,
            "url_count": row.url_count,
            "sample_url": row.sample_url
        })

    logging.info(f"Found {len(hashes)} common hashes. Saving to cache.")
    with open(HASHES_CACHE_FILE, "w") as f:
        json.dump({"date": str(max_date), "hashes": hashes}, f, indent=2)

    return hashes

def fetch_missing_contents(client, hashes, max_date, batch_size=100):
    """
    Downloads the actual JavaScript strings (contents) for the identified hashes.
    
    To optimize performance, this filters out hashes we already have cached locally,
    then executes batch queries against BigQuery (using WHERE hash IN (...)).
    Relies on BigQuery block-clustering by `hash` for cost efficiency.
    """
    missing_hashes = []
    for h_info in hashes:
        h = h_info["hash"]
        filepath = os.path.join(CONTENT_CACHE_DIR, f"{h}.txt")
        if not os.path.exists(filepath):
            missing_hashes.append(h)

    if not missing_hashes:
        return

    logging.info(f"Fetching content for {len(missing_hashes)} missing hashes in batches of {batch_size}...")

    for i in range(0, len(missing_hashes), batch_size):
        batch = missing_hashes[i:i+batch_size]
        hash_list = ", ".join([f"'{h}'" for h in batch])
        logging.info(f"Fetching batch {i // batch_size + 1}/{(len(missing_hashes) + batch_size - 1) // batch_size}...")
        
        query = f"""
        SELECT `hash`, ANY_VALUE(content) as content
        FROM `httparchive.crawl_staging.script_chunks`
        WHERE date = '{max_date}' AND `hash` IN ({hash_list})
        GROUP BY `hash`
        """
        job = client.query(query)
        fetched_count = 0
        for row in job:
            out_path = os.path.join(CONTENT_CACHE_DIR, f"{row.hash}.txt")
            with open(out_path, "wb") as f:
                content = row.content
                if content is not None:
                    f.write(content.encode("utf-8"))
            fetched_count += 1
            
        logging.debug(f"Fetched {fetched_count} contents in this batch.")

def read_content(hash_str):
    filepath = os.path.join(CONTENT_CACHE_DIR, f"{hash_str}.txt")
    if not os.path.exists(filepath):
        return None
    with open(filepath, "rb") as f:
        return f.read()

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"processed_index": 0, "hashes_included": 0, "dictionary_size": 0}

def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)

def main():
    """
    Primary execution flow:
    1. Determine the latest dataset timestamp.
    2. Identify the most widespread script hashes on the web.
    3. Pre-fetch script payloads locally.
    4. Iteratively evaluate each script chunk to build a highly optimized zstandard compression dictionary.
    """
    parser = argparse.ArgumentParser(description="Generate compression dictionary from common web scripts.")
    args = parser.parse_args()

    init_directories()

    # Step 1: Initialize BigQuery and determine the target dataset partition
    client = bigquery.Client()
    max_date = get_max_date(client)
    
    # Step 2: Extract top JS function clusters
    hashes = get_hashes(client, max_date)
    if not hashes:
        logging.info("No common hashes found.")
        return

    progress = load_progress()
    
    # Check if previously completed
    if progress["processed_index"] >= len(hashes) or progress.get("dictionary_size", 0) >= MAX_DICT_SIZE:
        logging.info("Previous run was marked as complete. Resetting progress to allow re-run.")
        progress = {"processed_index": 0, "hashes_included": 0, "dictionary_size": 0}
        if os.path.exists(DICT_FILE):
            os.remove(DICT_FILE)
            
    processed_index = progress["processed_index"]
    hashes_included = progress["hashes_included"]
    
    logging.info(f"Resuming from index {processed_index}. Total hashes to process: {len(hashes)}")

    # Load the existing dictionary state from disk
    if os.path.exists(DICT_FILE):
        with open(DICT_FILE, "rb") as f:
            dictionary_bytes = f.read()
    else:
        dictionary_bytes = b""

    # Step 3: Ensure all necessary script bodies are downloaded locally before starting evaluation
    # Pre-fetch all needed contents
    fetch_missing_contents(client, hashes[processed_index:], max_date)

    if processed_index == 0 and not dictionary_bytes:
        # Start the dictionary with the most common hash unconditionally
        h_info = hashes[0]
        logging.info(f"Starting dictionary with most common hash: {h_info['hash']}")
        content_bytes = read_content(h_info["hash"])
        if content_bytes is not None:
            dictionary_bytes += content_bytes
            with open(DICT_FILE, "wb") as f:
                f.write(dictionary_bytes)
            hashes_included += 1
        processed_index += 1
        progress = {
            "processed_index": processed_index,
            "hashes_included": hashes_included,
            "dictionary_size": len(dictionary_bytes)
        }
        save_progress(progress)

    # Step 4: Iteratively build the dictionary
    for i in range(processed_index, len(hashes)):
        # Stop execution once the cumulative dictionary hits the predetermined size limit
        if len(dictionary_bytes) >= MAX_DICT_SIZE:
            logging.info(f"Target dictionary size reached: {len(dictionary_bytes)} bytes. Stopping.")
            break

        h_info = hashes[i]
        h = h_info["hash"]
        content_bytes = read_content(h)

        if not content_bytes:
            # Maybe the fetch failed or was empty, just skip
            progress = {
                "processed_index": i + 1,
                "hashes_included": hashes_included,
                "dictionary_size": len(dictionary_bytes)
            }
            save_progress(progress)
            continue

        # Strategy Option A: Compress the chunk solely on its own (no initial dictionary context)
        cctx_nodict = zstandard.ZstdCompressor(level=11)
        nodict_compressed = cctx_nodict.compress(content_bytes)
        nodict_size = len(nodict_compressed)

        # Strategy Option B: Compress the chunk leveraging our current cumulative dictionary
        dict_data = zstandard.ZstdCompressionDict(dictionary_bytes)
        cctx_dict = zstandard.ZstdCompressor(level=11, dict_data=dict_data)
        dict_compressed = cctx_dict.compress(content_bytes)
        dict_size = len(dict_compressed)

        # Evaluation Decision: 
        # Only append this chunk to the cumulative dictionary if it introduces novel patterns.
        # We determine "novelty" if its dictionary-compressed size is > 50% of the normal compressed size.
        if dict_size > 0.5 * nodict_size:
            logging.info(f"[{i+1}/{len(hashes)}] Adding {h} to dict (nodict={nodict_size}, dict={dict_size}). Included: {hashes_included+1}")
            dictionary_bytes += content_bytes
            with open(DICT_FILE, "wb") as f:
                f.write(dictionary_bytes)
            hashes_included += 1
        else:
            logging.info(f"[{i+1}/{len(hashes)}] Skipping {h} (nodict={nodict_size}, dict={dict_size}).")

        progress = {
            "processed_index": i + 1,
            "hashes_included": hashes_included,
            "dictionary_size": len(dictionary_bytes)
        }
        save_progress(progress)

    logging.info(f"Done! Dictionary size: {len(dictionary_bytes)} bytes. Total hashes included: {hashes_included}.")

if __name__ == "__main__":
    main()
