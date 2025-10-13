#!/usr/bin/env python3
import os
import time
import json
import statistics
import pyarrow.parquet as pq
from torch.utils.data import DataLoader, Dataset


DATA_DIR = os.path.expanduser("~/projects/tensorfetch/data")
FILES = ["wide_1gb.parquet", "wide_2gb.parquet"]
OUTPUT_BASE = os.path.expanduser("~/projects/tensorfetch/results/logs")
os.makedirs(OUTPUT_BASE, exist_ok=True)


class ParquetShardDataset(Dataset):
    def __init__(self, data_dir, files):
        self.data_dir = data_dir
        self.files = files

    def __len__(self):
        return len(self.files)

    def __getitem__(self, idx):
        path = os.path.join(self.data_dir, self.files[idx])
        t0 = time.perf_counter()
        pf = pq.ParquetFile(path)       # triggers footer read
        _ = pf.metadata                 # parse metadata (footer read)
        t1 = time.perf_counter()
        return {
            "file": self.files[idx],
            "footer_time_ms": (t1 - t0) * 1000,
            "size_mb": os.path.getsize(path) / (1024 * 1024),
        }


def drop_caches():
    """Force clear Linux page cache for cold reads."""
    os.system("sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' >/dev/null 2>&1")


def run_baseline(num_workers: int, runs: int, tag: str):
    dataset = ParquetShardDataset(DATA_DIR, FILES)
    results_all = []

    for run_id in range(runs):
        print(f"\n===== Run {run_id + 1}/{runs} =====")
        drop_caches()
        time.sleep(1)

        loader = DataLoader(
            dataset,
            batch_size=1,
            shuffle=False,
            num_workers=num_workers,
            prefetch_factor=1,
        )

        run_results = []
        for batch in loader:
            file = batch["file"][0]
            t_ms = (
                batch["footer_time_ms"][0].item()
                if hasattr(batch["footer_time_ms"][0], "item")
                else batch["footer_time_ms"][0]
            )
            print(f"[Run {run_id + 1}] {file} footer {t_ms:.2f} ms")
            run_results.append({
                "file": file,
                "footer_time_ms": float(t_ms),
                "size_mb": float(batch["size_mb"][0]),
            })

        results_all.extend(run_results)

    # Aggregate per-file stats
    stats = {}
    for r in results_all:
        f = r["file"]
        stats.setdefault(f, []).append(r["footer_time_ms"])

    summary = []
    for f, times in stats.items():
        avg = statistics.mean(times)
        std = statistics.stdev(times) if len(times) > 1 else 0
        summary.append({
            "file": f,
            "avg_footer_time_ms": avg,
            "stddev_ms": std,
            "runs": len(times),
            "size_mb": os.path.getsize(os.path.join(DATA_DIR, f)) / (1024 * 1024),
        })
        print(f"{f} -> Avg: {avg:.2f} ms | Std: {std:.2f} ms")

    # Construct tagged output path
    tag_suffix = f"_{tag}" if tag else ""
    out_path = os.path.join(
        OUTPUT_BASE, f"dl_baseline{tag_suffix}_w{num_workers}_runs{runs}.jsonl"
    )
    with open(out_path, "w") as f:
        for r in summary:
            f.write(json.dumps(r) + "\n")

    print(f"Results saved to {out_path}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--runs", type=int, default=100)
    ap.add_argument("--tag", type=str, default="", help="Optional tag for output file name")
    args = ap.parse_args()

    print(f"Running baseline with {args.workers} workers, {args.runs} runs per file, tag={args.tag or 'none'}...")
    run_baseline(args.workers, args.runs, args.tag)