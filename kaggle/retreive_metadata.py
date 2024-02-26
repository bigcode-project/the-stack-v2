# code for getting metadata based on file id
import argparse
import json

import pandas as pd
from datasets import load_dataset

from utils.manual_sharding import save_manual_shards


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user_name", type=str, default="loubnabnl")
    return parser.parse_args()


ds = load_dataset("bigcode/kaggle-notebooks-data", use_auth_token=True, split="train")
print("dataset loaded")

# downloaded from https://www.kaggle.com/datasets/kaggle/meta-kaggle
kv_csv = "./metadata_kaggle/KernelVersions.csv"
kernelversions_datasetsources_csv = "./metadata_kaggle/KernelVersionDatasetSources.csv"
datasets_versions_csv = "./metadata_kaggle/DatasetVersions.csv"
datasets_csv = "./metadata_kaggle/Datasets.csv"
users_csv = "./metadata_kaggle/Users.csv"

kversions = pd.read_csv(kv_csv)
datasets_versions = pd.read_csv(datasets_versions_csv)
datasets = pd.read_csv(datasets_csv)
kernelversions_datasetsources = pd.read_csv(kernelversions_datasetsources_csv)
users = pd.read_csv(users_csv)
print("metadata loaded")


def safe_get(dataframe, condition, column=None):
    """Utility function to safely get value from DataFrame."""
    result = dataframe[condition]
    if result.empty:
        return None
    if column:
        return result[column].values[0]
    return result


def get_metadata(file_id):
    """given the id of a notebook (=the stem of its path) we retrieve metadata from csv tables
    provided by kaggle"""

    file_id_int = int(file_id)
    kversion = safe_get(kversions, kversions["Id"] == file_id_int)
    data_source_kernel = safe_get(
        kernelversions_datasetsources,
        kernelversions_datasetsources["KernelVersionId"] == file_id_int,
    )

    source_id = (
        None
        if data_source_kernel is None
        else data_source_kernel["SourceDatasetVersionId"].values[0]
    )
    dataset_v = safe_get(datasets_versions, datasets_versions["Id"] == source_id)

    data_name = dataset_v["Slug"].values[0] if dataset_v is not None else None
    dataset_id = dataset_v["DatasetId"].values[0] if dataset_v is not None else None

    source_dataset = safe_get(datasets, datasets["Id"] == dataset_id)
    owner_user_id = (
        None if source_dataset is None else source_dataset["OwnerUserId"].values[0]
    )

    user = safe_get(users, users["Id"] == owner_user_id)
    user_name = None if user is None else user["UserName"].values[0]

    return {
        "kaggle_dataset_name": data_name,
        "kaggle_dataset_owner": user_name,
        "kversion": json.dumps(kversion.to_dict(orient="records"))
        if kversion is not None
        else None,
        "kversion_datasetsources": json.dumps(
            data_source_kernel.to_dict(orient="records")
        )
        if data_source_kernel is not None
        else None,
        "dataset_versions": json.dumps(dataset_v.to_dict(orient="records"))
        if dataset_v is not None
        else None,
        "datasets": json.dumps(source_dataset.to_dict(orient="records"))
        if source_dataset is not None
        else None,
        "users": json.dumps(user.to_dict(orient="records"))
        if user is not None
        else None,
    }


def retrive_metadata(row):
    output = get_metadata(row["file_id"])
    return output


args = get_args()
# issue when using map with multipprocessing new values are None
new_ds = ds.map(retrive_metadata)
save_manual_shards(
    new_ds,
    user=args.user_name,
    remote_dataset_repo="kaggle-scripts-clean-dedup-meta",
)
subset = ds.select(range(1_000))
subset.push_to_hub("kaggle_scripts_subset")
print("Done! 💥")
