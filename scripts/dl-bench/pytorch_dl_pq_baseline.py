#!/usr/bin/env python3
import os, time, json, pyarrow.parquet as pq
from torch.utils.data import DataLoader, Dataset

DATA_DIR = "data"
FILES = ["wide_1gb.parquet", "wide_2gb.parquet"]
OUTPUT = "results/logs/dl_baseline.jsonl"
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

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
        _ = pf.metadata                 # parse metadata
        t1 = time.perf_counter()
        return {
            "file": self.files[idx],
            "footer_time_ms": (t1 - t0) * 1000,
            "size_mb": os.path.getsize(path) / (1024 * 1024),
        }

def run_baseline(num_workers: int):
    dataset = ParquetShardDataset(DATA_DIR, FILES)
    loader = DataLoader(
        dataset,
        batch_size=1,
        shuffle=True,
        num_workers=num_workers,
        prefetch_factor=1,
    )
    results = []
    for batch in loader:
        # batch is a dict of lists because default_collate stacked single-element dicts
        file = batch["file"][0]
        t_ms = batch["footer_time_ms"][0].item() if hasattr(batch["footer_time_ms"][0], "item") else batch["footer_time_ms"][0]
        print(f"[worker] {file} footer {t_ms:.2f} ms")
        results.append({
            "file": file,
            "footer_time_ms": t_ms,
            "size_mb": float(batch["size_mb"][0]),
        })
    out_path = OUTPUT.replace(".jsonl", f"_w{num_workers}.jsonl")
    with open(out_path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()
    os.system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'")
    print(f"Running baseline with {args.workers} workers...")
    run_baseline(args.workers)
