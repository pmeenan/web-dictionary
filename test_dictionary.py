#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import threading
import queue
import time
import gzip
import zlib
import concurrent.futures
import statistics
import zstandard
import subprocess

# Set environment
os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"

from google.cloud import bigquery
import requests
import zstandard
import brotli

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s")

CACHE_DIR = "cache"
DATA_DIR = "data"
HASHES_CACHE_FILE = os.path.join(CACHE_DIR, "hashes.json")
URLS_CACHE_FILE = os.path.join(CACHE_DIR, "test_urls.json")
RESULTS_FILE = os.path.join(DATA_DIR, "test_results.jsonl")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

def get_max_date():
    """Retrieve the active max date from hashes.json."""
    if not os.path.exists(HASHES_CACHE_FILE):
        logging.error(f"{HASHES_CACHE_FILE} not found. Please run generate_dictionary.py first.")
        sys.exit(1)
        
    with open(HASHES_CACHE_FILE, "r") as f:
        data = json.load(f)
        if "date" not in data:
            logging.error("No valid date found in hashes.json")
            sys.exit(1)
        return data["date"]

def get_urls(client, max_date):
    """Query BigQuery for target URLs if not already cached locally."""
    if os.path.exists(URLS_CACHE_FILE):
        logging.info("Loading cached URLs...")
        with open(URLS_CACHE_FILE, "r") as f:
            cached_data = json.load(f)
            if cached_data.get("date") == max_date:
                return cached_data["urls"]
            else:
                logging.info(f"Cached URLs date {cached_data.get('date')} does not match {max_date}. Re-querying.")

    logging.info(f"Querying httparchive.crawl.requests for date {max_date}...")
    
    query = f"""
    SELECT
      url,
      ANY_VALUE(type) as type,
      MIN(rank) as rank
    FROM
      `httparchive.crawl.requests`
    WHERE
      date = '{max_date}'
      AND type IN ('script', 'html')
      AND rank < 100000
    GROUP BY
      url
    ORDER BY
      rank ASC
    """
    job = client.query(query)
    urls = []
    for row in job:
        urls.append({
            "url": row.url,
            "type": row.type,
            "rank": row.rank
        })

    logging.info(f"Found {len(urls)} URLs. Saving to cache.")
    with open(URLS_CACHE_FILE, "w") as f:
        json.dump({"date": max_date, "urls": urls}, f, indent=2)

    return urls

def load_completed_urls():
    """Read the existing results file to skip already processed URLs."""
    completed = set()
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE, "r") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    if "url" in data:
                        completed.add(data["url"])
                except Exception:
                    continue
    return completed


def decompress_payload(raw_bytes, encoding):
    """Decompress raw bytes based on Content-Encoding header."""
    if not encoding or len(raw_bytes) == 0:
        return raw_bytes
    
    encoding = encoding.lower()
    try:
        if 'gzip' in encoding or 'x-gzip' in encoding:
            # windowBits 32 + max_wbits parses gzip headers automatically
            return zlib.decompress(raw_bytes, zlib.MAX_WBITS | 32)
        elif 'deflate' in encoding:
            # Some servers send raw deflate, others send zlib header
            try:
                return zlib.decompress(raw_bytes)
            except zlib.error:
                return zlib.decompress(raw_bytes, -zlib.MAX_WBITS)
        elif 'br' in encoding:
            return brotli.decompress(raw_bytes)
        elif 'zstd' in encoding:
            dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(raw_bytes)
    except Exception as e:
        logging.debug(f"Decompression failed for encoding {encoding}: {e}")
        return None
        
    return raw_bytes

def process_url(url_info, dict_file):
    """Fetch URL, measure raw encoded size, decompress, and run brotli (+ dict) tests."""
    url = url_info["url"]
    
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Encoding": "gzip, br, zstd"
    }

    try:
        # stream=True with decode_content=False to obtain raw bytes over the wire
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        
        # Determine actual payload size over wire
        raw_bytes = response.raw.read(decode_content=False)
        original_encoded_size = len(raw_bytes)
        status_code = response.status_code
        
        if status_code != 200 or original_encoded_size == 0:
            return {"url": url, "type": url_info["type"], "error": f"HTTP {status_code} or empty body"}

        if original_encoded_size <= 1400:
            return {"url": url, "type": url_info["type"], "error": "Size threshold not met"}

        encoding = response.headers.get("Content-Encoding", "")
        
        # Decompress full payload to calculate the decoded size reliably
        decoded_bytes = decompress_payload(raw_bytes, encoding)
        
        if decoded_bytes is None:
            return {"url": url, "type": url_info["type"], "error": "Decompression Failure"}
            
        full_decoded_size = len(decoded_bytes)
        
        # Avoid compressing empty content or excessively massive blocks
        if full_decoded_size == 0:
            return {"url": url, "type": url_info["type"], "error": "Empty decoded content"}
            
        if full_decoded_size > 50 * 1024 * 1024:
            return {"url": url, "type": url_info["type"], "error": "File too large to compress"}

        # Compress with standard brotli
        nodict_compressed = brotli.compress(decoded_bytes, quality=11)
        brotli_nodict_size = len(nodict_compressed)
        
        # Compress with dictionary brotli
        if dict_file:
            try:
                proc = subprocess.run(
                    ["brotli", "-q", "11", "-D", dict_file, "-c"],
                    input=decoded_bytes,
                    capture_output=True,
                    check=True
                )
                brotli_dict_size = len(proc.stdout)
            except subprocess.CalledProcessError as e:
                logging.error(f"Brotli command failed: {e.stderr.decode('utf-8', errors='ignore')}")
                brotli_dict_size = brotli_nodict_size
        else:
            brotli_dict_size = brotli_nodict_size

        return {
            "url": url,
            "type": url_info["type"],
            "rank": url_info["rank"],
            "original_encoded_size": original_encoded_size,
            "full_decoded_size": full_decoded_size,
            "brotli_11_size": brotli_nodict_size,
            "brotli_11_dict_size": brotli_dict_size
        }

    except Exception as e:
        return {"url": url, "type": url_info["type"], "error": str(e)}


def analyze_results():
    """Run statistics on the compiled results."""
    logging.info("Starting Analysis...")
    if not os.path.exists(RESULTS_FILE):
        logging.error("No results file to analyze.")
        return

    savings_list = {"script": [], "html": [], "all": []}
    total_processed = {"script": 0, "html": 0, "all": 0}
    total_errors = {"script": 0, "html": 0, "all": 0}

    with open(RESULTS_FILE, "r") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                req_type = data.get("type", "script")
                if req_type not in ["script", "html"]:
                    req_type = "script"
                
                if "error" in data:
                    total_errors["all"] += 1
                    total_errors[req_type] += 1
                    continue
                
                total_processed["all"] += 1
                total_processed[req_type] += 1
                
                z_size = data.get("brotli_11_size") or data.get("zstd_21_size") or data.get("zstd_19_size") or data.get("brotli_10_size")
                z_dict_size = data.get("brotli_11_dict_size") or data.get("zstd_21_dict_size") or data.get("zstd_19_dict_size") or data.get("brotli_10_dict_size")
                
                if z_size is not None and z_dict_size is not None and z_size > 0:
                    # Savings calculated relative to the standard compression overhead
                    # bytes saved / base comp size
                    savings = (z_size - z_dict_size) / z_size
                    savings_pct = savings * 100.0
                    savings_list["all"].append(savings_pct)
                    savings_list[req_type].append(savings_pct)
            except Exception:
                continue

    if not savings_list["all"]:
        logging.info("Not enough valid data points for analysis.")
        return

    for req_type in ["all", "script", "html"]:
        lst = savings_list[req_type]
        if not lst:
            continue
            
        lst.sort()
        
        avg_savings = sum(lst) / len(lst)
        
        # Calculate Percentiles using statistics module
        if len(lst) >= 2:
            try:
                pcts = statistics.quantiles(lst, n=100)
                p25 = pcts[24]
                p50 = pcts[49]
                p75 = pcts[74]
                p90 = pcts[89]
                p95 = pcts[94]
                p99 = pcts[98]
            except statistics.StatisticsError:
                # Fallback if too few elements
                p25 = p50 = p75 = p90 = p95 = p99 = avg_savings
        else:
            p25 = p50 = p75 = p90 = p95 = p99 = avg_savings

        logging.info(f"=== Analysis Results ({req_type.upper()}) ===")
        logging.info(f"Total Requests Processed: {total_processed[req_type]}")
        logging.info(f"Total Request Errors: {total_errors[req_type]}")
        logging.info(f"Avg URLs Saved Size: {avg_savings:.2f}%")
        logging.info("Savings Distribution against brotli level 11:")
        logging.info(f"  25th Percentile: {p25:.2f}%")
        logging.info(f"  50th Percentile: {p50:.2f}%")
        logging.info(f"  75th Percentile: {p75:.2f}%")
        logging.info(f"  90th Percentile: {p90:.2f}%")
        logging.info(f"  95th Percentile: {p95:.2f}%")
        logging.info(f"  99th Percentile: {p99:.2f}%")
        logging.info("========================")


def main():
    parser = argparse.ArgumentParser(description="Test dictionary compression.")
    parser.add_argument("--analyze", action="store_true", help="Only run the analysis on completed data")
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    if args.analyze:
        analyze_results()
        return

    max_date = get_max_date()
    year_month = str(max_date)[:7].replace("-", "")
    dict_file = os.path.join(DATA_DIR, f"{year_month}.dict")

    if os.path.exists(dict_file):
        dict_size = os.path.getsize(dict_file)
        logging.info(f"Loaded dictionary from {dict_file} ({dict_size} bytes)")
    else:
        dict_file = None
        logging.warning(f"No dictionary found at {dict_file}. Tests will run without a dictionary.")

    client = bigquery.Client()
    urls = get_urls(client, max_date)
    
    if not urls:
        logging.info("No URLs to process.")
        return

    completed_urls = load_completed_urls()
    pending_urls = [u for u in urls if u["url"] not in completed_urls]
    
    logging.info(f"{len(completed_urls)} URLs already processed. {len(pending_urls)} URLs remaining.")

    if pending_urls:
        logging.info("Starting processing pool...")
        processed_count = 0
        valid_count = {"script": 0, "html": 0, "all": 0}
        rolling_br_savings_sum = {"script": 0.0, "html": 0.0, "all": 0.0}
        rolling_br_dict_savings_sum = {"script": 0.0, "html": 0.0, "all": 0.0}
        total_pending = len(pending_urls)
        
        with open(RESULTS_FILE, "a") as f:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                active_futures = set()
                url_iter = iter(pending_urls)
                
                # Pre-fill active queue up to a reasonable bound
                for _ in range(min(50, total_pending)):
                    try:
                        target = next(url_iter)
                        active_futures.add(executor.submit(process_url, target, dict_file))
                    except StopIteration:
                        break
                        
                while active_futures:
                    # Wait for any future to complete
                    done, active_futures = concurrent.futures.wait(active_futures, return_when=concurrent.futures.FIRST_COMPLETED)
                    
                    for future in done:
                        result = future.result()
                        processed_count += 1
                        
                        if "error" not in result:
                            f.write(json.dumps(result) + "\n")
                            f.flush()
                            
                            orig_size = result.get("original_encoded_size")
                            # Try 11 first, else fallback mapped from old runs
                            br_size = result.get("brotli_11_size") or result.get("zstd_21_size") or result.get("zstd_19_size") or result.get("brotli_10_size")
                            br_dict_size = result.get("brotli_11_dict_size") or result.get("zstd_21_dict_size") or result.get("zstd_19_dict_size") or result.get("brotli_10_dict_size")
                            
                            if orig_size and orig_size > 0 and br_size is not None and br_dict_size is not None:
                                br_savings = ((orig_size - br_size) / orig_size) * 100.0
                                br_dict_savings = ((orig_size - br_dict_size) / orig_size) * 100.0
                                req_type = result.get("type", "script")
                                if req_type not in ["script", "html"]:
                                    req_type = "script"
                                
                                for t in ["all", req_type]:
                                    rolling_br_savings_sum[t] += br_savings
                                    rolling_br_dict_savings_sum[t] += br_dict_savings
                                    valid_count[t] += 1
                        
                        if processed_count % 100 == 0:
                            avg_msg = []
                            for t in ["all", "script", "html"]:
                                v = valid_count[t]
                                if v > 0:
                                    avg_br = (rolling_br_savings_sum[t] / v)
                                    avg_br_dict = (rolling_br_dict_savings_sum[t] / v)
                                    avg_msg.append(f"{t.upper()}: base {avg_br:.2f}%, dict {avg_br_dict:.2f}%")
                                    
                            status_str = " | ".join(avg_msg)
                            logging.info(f"Processed {processed_count} / {total_pending} URLs | {status_str}")
                            
                    # Refill the queue to keep workers busy
                    while len(active_futures) < 50:
                        try:
                            target = next(url_iter)
                            active_futures.add(executor.submit(process_url, target, dict_file))
                        except StopIteration:
                            break

        logging.info("Finished processing all pending URLs.")
    else:
        logging.info("All URLs have already been processed.")

    # Always run analysis at the end
    analyze_results()

if __name__ == "__main__":
    main()
