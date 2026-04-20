#!/usr/bin/env python3
"""Download and extract the Snowset parquet archives."""

from __future__ import annotations

import argparse
import shutil
import tarfile
import urllib.request
from pathlib import Path


DATASET_URLS = {
    "snowset-main.parquet.tar.gz": "http://www.cs.cornell.edu/~midhul/snowset/snowset-main.parquet.tar.gz",
    "ts-explosion.parquet.tar.gz": "http://www.cs.cornell.edu/~midhul/snowset/ts-explosion.parquet.tar.gz",
}


def _is_within_directory(directory: Path, target: Path) -> bool:
    try:
        target.relative_to(directory)
        return True
    except ValueError:
        return False


def download_file(url: str, destination: Path, force: bool) -> None:
    if destination.exists() and not force:
        print(f"Skipping download, file already exists: {destination.name}")
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} -> {destination}")
    with urllib.request.urlopen(url) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def extract_archive(archive_path: Path, output_dir: Path) -> None:
    if not archive_path.exists():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive.getmembers():
            member_path = output_dir / member.name
            if not _is_within_directory(output_dir, member_path.resolve()):
                raise ValueError(f"Unsafe path in archive: {member.name}")
        archive.extractall(path=output_dir)

    print(f"Extracted {archive_path.name} to {output_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download and extract the official Snowset parquet archives."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory where the downloaded archives and extracted parquet directories will be stored.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and only extract archives already present in --output-dir.",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Download archives but do not extract them.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download archives even if they already exist.",
    )
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    archive_paths: list[Path] = []
    for filename, url in DATASET_URLS.items():
        archive_path = output_dir / filename
        archive_paths.append(archive_path)
        if not args.skip_download:
            download_file(url, archive_path, force=args.force_download)

    if args.skip_extract:
        return

    for archive_path in archive_paths:
        extract_archive(archive_path, output_dir)


if __name__ == "__main__":
    main()
