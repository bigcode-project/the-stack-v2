import boto3
import requests
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)
from pyspark import StorageLevel

repo_paths_schema = StructType(
    [
        StructField("directory_id", StringType(), False),
        StructField("path", StringType(), False),
        StructField("content_id", StringType(), False),
    ]
)

file_contents_schema = StructType(
    [
        StructField("blob_id", StringType(), False),
        StructField("licenses", ArrayType(StringType()), True),
    ]
)


permissive_whitelist = set(
    requests.get(
        "https://huggingface.co/datasets/bigcode-data/license_list/resolve/main/permissive_licenses.txt"
    )
    .text.strip()
    .split("\n")
)
# amendments to licenses that don't affect the license type
non_license_whitelist = {
    "LicenseRef-scancode-dco-1.1",
    "LicenseRef-scancode-generic-cla",
    "LicenseRef-scancode-google-cla",
    "LicenseRef-scancode-jetty-ccla-1.1",
    "LicenseRef-scancode-newton-king-cla",
    "LicenseRef-scancode-generic-exception",
    "LicenseRef-scancode-generic-export-compliance",
    "LicenseRef-scancode-generic-tos",
    "LicenseRef-scancode-generic-trademark",
    "LicenseRef-scancode-warranty-disclaimer",
}


def get_license_type(gha_license_id, detected_licenses):
    if gha_license_id in permissive_whitelist:
        return "permissive"
    elif gha_license_id != "NOASSERTION" and gha_license_id is not None:
        return "non_permissive"

    if len(detected_licenses) == 0:
        if gha_license_id is None:
            return "no_license"
        else:
            return "non_permissive"

    permissive = permissive_whitelist.intersection(detected_licenses)
    extras = non_license_whitelist.intersection(detected_licenses)
    non_permissive = set(detected_licenses) - permissive.union(extras)
    if len(permissive) > 0:
        # the license is known, just linked from one of the files
        non_permissive.discard("LicenseRef-scancode-unknown-license-reference")

    if len(permissive) > 0 and len(non_permissive) == 0:
        return "permissive"
    elif len(non_permissive) == 0:
        return "no_license"
    else:
        return "non_permissive"


def main(spark, args):
    repos_df = spark.read.parquet(args.repo_data_path)
    repos_df = repos_df.selectExpr(
        "full_name as repo_name",
        "revision_date",
        "visit_date",
        "star_events_count",
        "fork_events_count",
        "license_spdx_id as gha_license_id",
        "directory_id",
    )

    repo_paths = spark.read.parquet(args.file_paths_path).select(
        "directory_id", "path", "content_id", "blob_id"
    )

    #######
    # Only fetching blob_id and licenses from the file contents
    file_contents = spark.read.json(
        args.file_contents_path,
        schema=file_contents_schema,
    ).drop("blob_prefix")

    licensed_files = repo_paths.join(file_contents, "blob_id").persist(
        StorageLevel.DISK_ONLY
    )

    ########
    # Aggregate licenses by base path
    licensed_base_paths = (
        licensed_files.filter(F.size(F.col("licenses")) > 0)
        .withColumn("base_path", F.regexp_extract(licensed_files.path, "^(.*/)", 1))
        .groupBy(
            "directory_id",
            "base_path",
        )
        .agg(
            F.array_distinct(F.flatten(F.collect_list("licenses"))).alias(
                "detected_licenses"
            )
        )
    )

    # EXACT DEDUP
    licensed_files = (
        licensed_files.join(
            repos_df.select(
                "directory_id",
                "gha_license_id",
                "star_events_count",
                "fork_events_count",
                "revision_date",
                "visit_date",
            ),
            "directory_id",
        )
        .withColumn("is_permissive", F.col("gha_license_id").isin(permissive_whitelist))
        .drop("gha_license_id")
    )
    # during exact dedup, prefer permissively licensed files from more popular repos
    repo_window = Window.partitionBy("blob_id").orderBy(
        F.desc_nulls_last("is_permissive"),
        F.desc_nulls_last("star_events_count"),
        F.desc_nulls_last("fork_events_count"),
        F.desc_nulls_last("revision_date"),
        F.desc_nulls_last("visit_date"),
    )
    licensed_files = licensed_files.withColumn(
        "repo_priority", F.row_number().over(repo_window)
    )
    licensed_files = licensed_files.filter(F.col("repo_priority") == 1).drop(
        "repo_priority",
        "star_events_count",
        "fork_events_count",
        "revision_date",
        "visit_date",
        "is_permissive",
    )

    # salted join to avoid skew
    SALT_PARTITIONS = 8
    licensed_files = licensed_files.withColumn(
        "salt", F.expr(f"CAST(floor(rand() * {SALT_PARTITIONS}) AS INT)")
    )
    licensed_base_paths = licensed_base_paths.withColumn(
        "salt", F.array([F.lit(num) for num in range(0, SALT_PARTITIONS)])
    ).withColumn("salt", F.explode(F.col("salt")))
    licensed_files = licensed_files.alias("left").join(
        licensed_base_paths.alias("right"),
        (F.col("left.salt") == F.col("right.salt"))
        & (F.col("left.directory_id") == F.col("right.directory_id"))
        & F.col("left.path").startswith(F.col("right.base_path")),
        how="left",
    )
    licensed_files = licensed_files.drop("salt", "base_path")
    licensed_files = licensed_files.groupBy(
        "left.directory_id",
        "left.path",
        "left.content_id",
        "left.blob_id",
    ).agg(
        F.array_distinct(F.flatten(F.collect_list("right.detected_licenses"))).alias(
            "detected_licenses"
        )
    )
    licensed_files = licensed_files.join(repos_df, "directory_id")

    get_license_type_udf = F.udf(get_license_type, StringType())
    licensed_files = licensed_files.withColumn(
        "license_type",
        get_license_type_udf(F.col("gha_license_id"), F.col("detected_licenses")),
    )

    licensed_files.write.format("parquet").option("compression", "snappy").mode(
        "overwrite"
    ).save(args.output_path)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--repo_data_path",
        default="s3a://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/repo_data/",
        type=str,
        help="S3 path to the repository metadata",
    )
    args.add_argument(
        "--file_paths_path",
        default="s3a://bigcode-datasets-us-east-1/swh_2023_09_06/file_paths/",
        type=str,
        help="S3 path to the file paths dataset",
    )
    args.add_argument(
        "--file_contents_path",
        default="s3a://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/file_contents/",
        type=str,
        help="S3 path to the file contents dataset",
    )
    args.add_argument(
        "--output_path",
        default="s3a://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/detected_licenses/",
        type=str,
        help="S3 path to the unique files dataset",
    )
    args = args.parse_args()

    aws_creds = boto3.Session().get_credentials()
    spark = (
        SparkSession.builder.config("spark.sql.shuffle.partitions", 65536)
        .config("spark.hadoop.fs.s3a.access.key", aws_creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_creds.secret_key)
        .appName("get_license_types")
        .getOrCreate()
    )

    main(spark, args)
