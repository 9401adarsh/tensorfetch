import os
import io
from PIL import Image
import pandas as pd 
import numpy as np 
import pyarrow as pa
import pyarrow.parquet as pq

root = "data/tiny-imagenet/tiny-imagenet-200/train/"
out_dir = "data/tiny-imagenet-pq/"
os.makedirs(out_dir, exist_ok=True)

shard_size = 500
rows = []
shard_id = 0

for cls in sorted(os.listdir(root)):
    cls_dir = os.path.join(root, cls, "images")
    if not os.path.isdir(cls_dir):
        continue
    for img_name in os.listdir(cls_dir):
        img_path = os.path.join(cls_dir, img_name)
        try:
            with open(img_path, 'rb') as f:
                img_bytes = f.read()
            rows.append({'class': cls, 'data': img_bytes, 'filename': img_name})
        except Exception as e:
            print(f"Error reading {img_path}: {e}")
            continue
        
        if len(rows) >= shard_size:
            table = pa.Table.from_pandas(pd.DataFrame(rows))
            out_path = os.path.join(out_dir, f"shard_{shard_id:04d}.parquet")
            pq.write_table(table, out_path, compression='zstd')
            print(f"Wrote {out_path} with {len(rows)} images.")
            shard_id += 1
            rows.clear()

if rows:
    table = pa.Table.from_pandas(pd.DataFrame(rows))
    out_path = os.path.join(out_dir, f"shard_{shard_id:04d}.parquet")
    pq.write_table(table, out_path, compression='zstd')
    print(f"Wrote {out_path} with {len(rows)} images.")
