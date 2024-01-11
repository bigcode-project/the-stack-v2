import argparse
import boto3

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import StorageLevel


def main(spark, args):
    directories = (
        spark.read.parquet(args.repo_data_path)
        .selectExpr("directory_id as root_dir")
        .withColumn("path", F.lit(""))
        .withColumn("node", F.col("root_dir"))
        .repartition("node")
    )

    tree = (
        spark.read.orc(args.swh_directory_entry_path)
        .selectExpr("directory_id as parent", "name", "type", "target as child")
        .filter(F.col("type") != "rev")
        .withColumn("is_file", F.col("type") == "file")
        .drop("type")
        # file/dir name is in bytes
        .withColumn("name", F.decode(F.col("name"), "utf-8"))
        .repartition("parent")
        .persist(StorageLevel.DISK_ONLY)
    )

    content = (
        spark.read.orc(args.swh_content_path)
        .selectExpr("sha1 as blob_id", "sha1_git as content_id", "length")
        .repartition("content_id")
        .persist(StorageLevel.DISK_ONLY)
    )

    for level in range(64):
        node_window = Window.partitionBy("node").orderBy(F.col("root_dir"))
        directories_next = (
            directories
            # limit the number of files/subdirs in a directory to 1M (avoid a skewed join)
            .withColumn("node_num", F.row_number().over(node_window))
            .filter(F.col("node_num") <= 1000000)
            .drop("node_num")
        )
        directories_next = (
            directories_next.join(tree, F.col("node") == F.col("parent"))
            .withColumn("path", F.concat(F.col("path"), F.lit("/"), F.col("name")))
            .selectExpr("root_dir", "path", "child as node", "is_file")
        )

        # cache the paths so far to avoid recomputing them in case of node failure
        directories_next.repartition(512).write.format("parquet").mode(
            "overwrite"
        ).save(f"{args.cache_path}/level={level}/")
        directories_next.unpersist()
        directories = spark.read.parquet(f"{args.cache_path}/level={level}/")

        file_paths = (
            directories.filter(F.col("is_file"))
            .drop("is_file")
            .withColumnRenamed("root_dir", "directory_id")
            .withColumnRenamed("node", "content_id")
        )
        file_paths = file_paths.join(content, "content_id").repartition(512)

        file_paths.write.format("parquet").option("compression", "snappy").mode(
            "overwrite"
        ).save(f"{args.output_path}/level={level}/")

        directories = directories.filter(~F.col("is_file")).drop("is_file")


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--repo_data_path",
        default="s3a://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/repo_data/",
        type=str,
        help="S3 path to the repository metadata",
    )
    args.add_argument(
        "--swh_directory_entry_path",
        default="s3a://softwareheritage/graph/2023-09-06/orc/directory_entry/",
        type=str,
        help="S3 path to SWH directory_entry orc files",
    )
    args.add_argument(
        "--swh_content_path",
        default="s3a://softwareheritage/graph/2023-09-06/orc/content/",
        type=str,
        help="S3 path to SWH content orc files",
    )
    args.add_argument(
        "--cache_path",
        default="s3a://bigcode-datasets-us-east-1/swh_2023_09_06/file_paths_cache",
        type=str,
        help="S3 path to the directory paths cache",
    )
    args.add_argument(
        "--output_path",
        default="s3a://bigcode-datasets/swh_2023_09_06/file_paths",
        type=str,
        help="S3 path to save the final file paths",
    )
    args = args.parse_args()

    aws_creds = boto3.Session().get_credentials()
    spark = (
        SparkSession.builder.config("spark.sql.shuffle.partitions", 65536)
        .config("spark.hadoop.fs.s3a.access.key", aws_creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_creds.secret_key)
        .appName("find_file_paths")
        .getOrCreate()
    )

    main(spark, args)
