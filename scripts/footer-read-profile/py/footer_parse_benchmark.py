#!/usr/bin/env python3
import os, subprocess, time, mmap, struct, statistics
import pyarrow.parquet as pq
import thriftpy2
from thriftpy2.protocol.compact import TCompactProtocol
from thriftpy2.transport import TMemoryBuffer

# === CONFIG ===
DATA_DIR = "~/projects/tensorfetch/data"
FILES = ["wide_2gb.parquet"]
RUNS = 1
DROP_CACHE = False

DATA_DIR = os.path.expanduser(DATA_DIR)

# === Load Parquet Thrift schema ===
parquet_thrift = thriftpy2.load(
    "parquet.thrift",
    module_name="parquet_thrift"
)

def drop_caches():
    """Clear Linux page cache for cold reads."""
    os.system("sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'")


# === Full Parse using PyArrow ===
def parse_footer_full(path: str):
    t0 = time.perf_counter()
    with pq.ParquetFile(path) as pf:
        metadata_start = time.perf_counter()
        _ = pf.metadata
        metadata_end = time.perf_counter()
        print(f"Full parse metadata read time: {1000 * (metadata_end - metadata_start)}  milli-seconds.")
    t1 = time.perf_counter()
    return (t1 - t0) * 1000


# === Selective Parse: read only footer bytes, deserialize minimal metadata ===
def parse_footer_selective(path: str, bin_path: str = "footer_parse_helper"):
    #path: to selective pars
    res = subprocess.run([bin_path, path, str(1)], capture_output=True, text=True)
    line = res.stdout.strip()
    print(f"Selective parse output: {line}")
    return float(line.strip())

# === Benchmark Loop ===
def run_benchmark(bin_path: str = "footer_parse_helper"):
    results = {f: {"full": [], "selective": []} for f in FILES}

    for run in range(1, RUNS + 1):
        for f in FILES:
            if DROP_CACHE:
                drop_caches()
                time.sleep(0.05)  # wait a bit for cache to clear
            path = os.path.join(DATA_DIR, f)
            full_ms = parse_footer_full(path)
            results[f]["full"].append(full_ms)
    
    # for run in range(1, RUNS + 1):
    #     for f in FILES:
    #         if DROP_CACHE:
    #             drop_caches()
    #             time.sleep(0.05)  # wait a bit for cache to clear
    #         path = os.path.join(DATA_DIR, f)
    #         sel_ms = parse_footer_selective(path, bin_path=bin_path)
    #         results[f]["selective"].append(sel_ms)
    
    print("\n===== Averages =====")
    for f in FILES:
        full_avg = statistics.mean(results[f]["full"])
        print(f"{f}: full={full_avg:.2f} ms")

    return results


if __name__ == "__main__":
    # Ensure parquet.thrift is available
    if not os.path.exists("parquet.thrift"):
        print("ERROR: Missing parquet.thrift — download it from Apache Arrow repo:")
        print("https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift")
        exit(1)

    bin_path = "../cpp/footer_bench"
    run_benchmark(bin_path=bin_path)