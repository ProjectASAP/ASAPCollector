import pandas as pd
from pathlib import Path


def clean_data(input_path: Path, output_path: Path):
    df = pd.read_csv(input_path)

    # drop rows with missing timestamp
    df = df.dropna(subset=["t"])

    # drop columns with too many NaN
    thresh = int(0.9 * len(df))
    df = df.dropna(axis=1, thresh=thresh)

    # fill remaining NaN
    df = df.fillna(method="ffill").fillna(0)

    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    clean_data(
        Path("data/1_0_10000_17.csv"),
        Path("data_filtered/1_0_10000_17.csv"),
    )