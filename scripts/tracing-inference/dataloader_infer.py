import torch
import torch.nn as nn
import pyarrow.parquet as pq
import time
import os, subprocess


# --- dummy model --- (200 input columns, as in wide_1gb.parquet)
model = nn.Sequential(
    nn.Linear(200, 1024),
    nn.ReLU(),
    nn.Linear(1024, 1024),
    nn.ReLU(),
    nn.Linear(1024, 512),
    nn.ReLU(),
    nn.Linear(512, 256),
    nn.ReLU(),
    nn.Linear(256, 64),
    nn.ReLU(),
    nn.Linear(64, 1)
).eval()


# --- custom Dataset using PyArrow ---
class ParquetRowGroupDataset(torch.utils.data.Dataset):
    """Each item = one Parquet row-group (features tensor, labels tensor)."""
    def __init__(self, path):
        self.path = path
        self.pf = pq.ParquetFile(path)

    def __len__(self):
        return self.pf.num_row_groups

    def __getitem__(self, idx):
        table = self.pf.read_row_group(idx)
        df = table.to_pandas()

        # handle absence of 'label' column gracefully
        if "label" in df.columns:
            features_df = df.drop(columns=["label"])
            label = torch.tensor(df["label"].values, dtype=torch.float32).unsqueeze(1)
        else:
            features_df = df
            label = None

        features = torch.tensor(features_df.values, dtype=torch.float32)
        return features, label


# --- inference loop ---
def run_inference_loop(datafile, model, num_workers=0):
    print(f"\nRunning inference loop on {datafile} ...")
    dataset = ParquetRowGroupDataset(datafile)
    loader = torch.utils.data.DataLoader(
        dataset, batch_size=None, num_workers=num_workers, prefetch_factor=4
    )

    start_all = time.perf_counter()
    for idx, (features, label) in enumerate(loader):
        with torch.no_grad():
            print(f"  Inference on row-group {idx + 1}/{len(dataset)} ...", end="\r")
            inference_start = time.perf_counter()
            _ = model(features)
            inference_end = time.perf_counter()
            print(f"  Inference on row-group {idx + 1}/{len(dataset)} took {inference_end - inference_start:.3f} seconds.")
    end_all = time.perf_counter()
    total_time = end_all - start_all
    print(f"  Completed {idx + 1} row-groups in {total_time:.3f} seconds.")


# --- main driver ---
if __name__ == "__main__":
    datafiles = [
        "~/projects/tensorfetch/data/wide_1gb.parquet",
        "~/projects/tensorfetch/data/wide_2gb.parquet",
    ]
    for datafile in datafiles:
        datafile = os.path.expanduser(datafile)  #
        fd = os.open(datafile, os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        os.close(fd)
        run_inference_loop(datafile, model, num_workers=8)