#!/usr/bin/env python3
import os
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np

# ------------------------------------
# Setup directories
# ------------------------------------
os.makedirs("data", exist_ok=True)
os.makedirs("data/fhv-2019", exist_ok=True)

# ------------------------------------
# 1. Download FHV 2019 Parquet files
# ------------------------------------
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
months = [f"2019-{str(m).zfill(2)}" for m in range(1, 13)]
fhv_downloaded = []

print("Downloading FHV Parquet files for 2019 ...")
for m in months:
    fname = f"data/fhv-2019/fhv_tripdata_{m}.parquet"
    if not os.path.exists(fname):
        print(f"  Fetching {m} ...")
        os.system(f"wget -q {base_url}/fhv_tripdata_{m}.parquet -O {fname}")
    if os.path.exists(fname):
        size_mb = os.path.getsize(fname) / (1024 * 1024)
        print(f"  Downloaded {os.path.basename(fname)} ({size_mb:.1f} MB)")
        fhv_downloaded.append(fname)
    else:
        print(f"  Skipped {m} (download failed)")

if not fhv_downloaded:
    raise RuntimeError("No FHV Parquet files downloaded; check network access.")

print(f"Downloaded {len(fhv_downloaded)} monthly FHV Parquet files into data/fhv-2019/")

# ------------------------------------
# 2. Generate synthetic wide Parquet datasets (1 GB, 2 GB, 4 GB)
# ------------------------------------
def generate_wide_parquet(rows, cols, path):
    print(f"Generating {path} ...")
    df = pd.DataFrame({f"col_{i}": np.random.rand(rows) for i in range(cols)})
    pq.write_table(pa.Table.from_pandas(df), path, row_group_size=50_000)
    size_gb = os.path.getsize(path) / (1024 ** 3)
    print(f"  Created {path} ({size_gb:.2f} GB)")

# Approximate scaling: 1 M rows ≈ 1 GB for 200 float columns
generate_wide_parquet(1_000_000, 200, "data/wide_1gb.parquet")
generate_wide_parquet(2_000_000, 200, "data/wide_2gb.parquet")

# ------------------------------------
# 3. Report footer sizes
# ------------------------------------
print("\nFooter sizes summary:")
targets = [
    "data/wide_1gb.parquet",
    "data/wide_2gb.parquet",
]

for f in targets:
    if not os.path.exists(f):
        continue
    pf = pq.ParquetFile(f)
    footer_kb = pf.metadata.serialized_size / 1024
    print(
        f"  {f}: footer ≈ {footer_kb:.1f} KB, "
        f"rows = {pf.metadata.num_rows:,}"
    )
