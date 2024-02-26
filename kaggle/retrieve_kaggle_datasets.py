"""
download dataset from kaggle
make sure you have kaggle.json in ~/.kaggle/kaggle.json
Example:
 python retreive_dataset.py --total-slices 4000 --ith-slice 0 --get-datainfo --download-data

 (skip --get-datainfo to just download data first)
"""
import argparse
import ast
import json
import os
import re
import shutil
from itertools import islice

import pandas as pd
from datasets import load_dataset
from func_timeout import FunctionTimedOut, func_timeout
from termcolor import colored
from tqdm import tqdm

import kaggle

DATADIR = "./data_dir"
TIMEOUT = 120  # seconds


PATTERNS = [
    r"\.\./input/((?:[^/]+/)+[^/]+\.csv)",  # Matches '../input/.../....csv'
    r"kaggle/input/((?:[^/]+/)+[^/]+\.csv)",  # Matches 'kaggle/input/.../....csv'
    r"/kaggle/input/((?:[^/]+/)+[^/]+\.csv)",  # Matches '/kaggle/input/.../....csv'
]


def extract_relative_file_paths(filepaths, patterns):
    extracted_paths = []

    for filepath in filepaths:
        for pattern in patterns:
            match = re.search(pattern, filepath)
            if match:
                extracted_paths.append(match.group(1))
                break

    return list(set(extracted_paths))  # Removing duplicates


def is_pathname_valid(pathname: str) -> bool:
    """
    `True` if the passed string satisfies at least one pattern,
    `False` otherwise.
    """
    for pattern in PATTERNS:
        if re.search(pattern, pathname):
            return True
    return False


def extract_read_csv_filenames(code: str):
    # remove all lines start with "%"
    code_preprocessed = ""
    for line in code.splitlines():
        if line.startswith("%"):
            continue
        code_preprocessed += line + "\n"

    # Parse the code using ast.parse
    try:
        parsed_code = ast.parse(code)
    except Exception as e:
        # print(e)
        return []

    # List to store extracted file paths
    file_paths = []

    # Traverse the AST
    for node in ast.walk(parsed_code):
        # Check if the node is a function call
        if isinstance(node, ast.Call):
            # Check if the function being called is read_csv
            if hasattr(node.func, "attr") and node.func.attr == "read_csv":
                # Extract the first argument (file path)
                if len(node.args) > 0 and isinstance(node.args[0], ast.Str):
                    file_paths.append(node.args[0].s)
    ret = []
    for file_path in file_paths:
        file_path = file_path.strip()
        if is_pathname_valid(file_path):
            ret.append(file_path)
    return ret


def download_data(dataset_ref: str, api, download_path, timeout_seconds=60):
    # Create download path if it doesn't exist
    os.makedirs(download_path, exist_ok=True)
    print(f"downloading to {download_path}")

    try:
        # Set a timeout for the download function
        func_timeout(
            timeout_seconds,
            api.dataset_download_files,
            args=(dataset_ref,),
            kwargs={"path": download_path, "unzip": True},
        )
    except FunctionTimedOut:
        print(f"download of {dataset_ref} timed out after {timeout_seconds} seconds")
        shutil.rmtree(download_path)  # Cleanup the partial download
    except Exception as e:
        print(e)
        shutil.rmtree(download_path)  # Cleanup the partial download
    else:
        print(f"downloaded {dataset_ref} with no exception")


def main():
    # initialize kaggle api
    api = kaggle.KaggleApi()
    api.authenticate()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--download-data", action="store_true", help="whether to download data"
    )
    parser.add_argument(
        "--get-datainfo",
        action="store_true",
        help="whether to get data info from downloaded csv files",
    )

    # Argument for the total number of slices
    parser.add_argument(
        "--total-slices",
        type=int,
        required=True,
        help="Total number of slices to divide the dataset into.",
    )
    # Argument for the ith slice we want to process
    parser.add_argument(
        "--ith-slice",
        type=int,
        required=True,
        help="(0-index) The ith slice of the dataset you want to process.",
    )

    args = parser.parse_args()
    args = parser.parse_args()

    error_counter = 0
    get_cloumn_notebook_counter = 0

    notebook_dataset = load_dataset(
        "bigcode/kaggle_scripts_final", split="train", num_proc=64
    )

    dataset_length = len(notebook_dataset)
    slice_size = dataset_length // args.total_slices

    start_idx = slice_size * (args.ith_slice)
    end_idx = (
        start_idx + slice_size if args.ith_slice != args.total_slices - 1 else None
    )

    for data in tqdm(islice(notebook_dataset, start_idx, end_idx)):
        notebook = json.loads(data["content"])
        file_id = data["file_id"]
        dataset_ref = None
        if data["kaggle_dataset_owner"] and data["kaggle_dataset_name"]:
            dataset_ref = (
                f"{data['kaggle_dataset_owner']}/{data['kaggle_dataset_name']}"
            )

        if dataset_ref:
            print(dataset_ref)
        else:
            continue

        # Check if the notebook has csv matched pattern
        filenames = []
        for cell in notebook:
            if cell["cell_type"] == "code":
                if "read_csv(" in cell["source"]:
                    filenames += extract_read_csv_filenames(cell["source"])

        if args.download_data and dataset_ref and filenames:
            print(f"try downloading data {dataset_ref}")
            download_path = f"{DATADIR}/{dataset_ref}"
            if os.path.exists(download_path):
                print(f"{download_path} exists")
                continue

            download_data(dataset_ref, api, download_path)

        if not args.get_datainfo:
            # exit early if we don't need to retrieve data info now
            continue

        filenames = extract_relative_file_paths(filenames, PATTERNS)
        # print(filenames)
        get_column_flag = False

        for filename in filenames:
            try:
                df = pd.read_csv(f"{DATADIR}/{dataset_ref}/../{filename}")
                print(colored("csv file:", "red"), filename)
                print(colored("columns:", "red"), df.columns)
                df_summary_json = df.head(5).to_json()
                # TODO: decide what to store

            except Exception as e:
                # print(e)
                pass
            else:
                get_column_flag = True

        if get_column_flag:
            get_cloumn_notebook_counter += 1

    print(f"total number of notebooks in this slice: {slice_size}")
    print(
        f"total number of notebooks that obtain corresponding dataframe: {get_cloumn_notebook_counter}"
    )


if __name__ == "__main__":
    main()
