import argparse
import boto3

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.window import Window
from pyspark import StorageLevel

license_schema = StructType(
    [
        StructField("key", StringType(), True),
        StructField("name", StringType(), True),
        StructField("spdx_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("node_id", StringType(), True),
    ]
)

repo_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("full_name", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("mirror_url", StringType(), True),
        StructField("default_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("fork", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("pushed_at", TimestampType(), True),
        StructField("homepage", StringType(), True),
        StructField("size", LongType(), True),
        StructField("stargazers_count", IntegerType(), True),
        StructField("forks_count", IntegerType(), True),
        StructField("open_issues_count", IntegerType(), True),
        StructField("language", StringType(), True),
        StructField("has_issues", BooleanType(), True),
        StructField("has_projects", BooleanType(), True),
        StructField("has_downloads", BooleanType(), True),
        StructField("has_wiki", BooleanType(), True),
        StructField("has_pages", BooleanType(), True),
        StructField("has_discussions", BooleanType(), True),
        StructField("archived", BooleanType(), True),
        StructField("disabled", BooleanType(), True),
        StructField("license", license_schema, True),
    ]
)

# fmt: off
event_schema = StructType([
    StructField('type', StringType(), True),
    StructField('created_at', TimestampType(), True),
    StructField('repo', StructType([
        StructField('id', LongType(), True),
        StructField('name', StringType(), True),
    ]), True),
    StructField('payload',
                StructType([
                    StructField('pull_request',
                                StructType([
                                    StructField('base',
                                                StructType([
                                                    StructField('repo', repo_schema, True)
                                                ]), True)
                                ]), True),
                    StructField('forkee', repo_schema, True)
                ]), True)
])
# fmt: on


def read_orc_sanitize_timestamps(spark, path):
    schema = spark.read.orc(path).schema
    cols_to_sanitize = []
    for el in schema:
        if isinstance(el.dataType, TimestampType) or isinstance(
            el.dataType, TimestampNTZType
        ):
            el.dataType = StringType()
            cols_to_sanitize.append(el.name)
    df = spark.read.schema(schema).orc(path)
    for el in cols_to_sanitize:
        df = df.withColumn(el, F.to_timestamp(el))
    return df


def get_github_repo_data(spark, path):
    gha_events = spark.read.json(path, schema=event_schema).persist(
        StorageLevel.DISK_ONLY
    )

    id_names = (
        gha_events.selectExpr("repo.id as github_id", "repo.name as full_name")
        .filter("github_id is not null and full_name is not null")
        .dropDuplicates(["github_id", "full_name"])
    )

    def process_event_data(event_type, count_alias):
        return (
            gha_events.filter(F.col("type") == event_type)
            .selectExpr(
                "repo.id as github_id",
                "repo.name as full_name",
            )
            .groupBy("github_id", "full_name")
            .agg(F.count(F.lit(1)).alias(count_alias))
        )

    # watch events = starring events. un-starring is not recorded, so this might be slightly more than actual counts
    watch_events = process_event_data("WatchEvent", "star_events_count")
    fork_events = process_event_data("ForkEvent", "fork_events_count")

    pr_repos = gha_events.filter("type = 'PullRequestEvent'").selectExpr(
        "created_at as event_created_at", "payload.pull_request.base.repo.*"
    )
    fork_repos = gha_events.filter("type = 'ForkEvent'").selectExpr(
        "created_at as event_created_at", "payload.forkee.*"
    )
    repo_data = pr_repos.union(fork_repos)

    # select only the latest repo info
    window = Window.partitionBy("id").orderBy(F.desc("event_created_at"))
    repo_data = (
        repo_data.withColumn("row_number", F.row_number().over(window))
        .filter("row_number = 1")
        .drop("row_number")
    )
    repo_data = repo_data.withColumnRenamed("id", "github_id")
    metadata_cols = [
        F.col(c) for c in repo_data.columns if c not in ["github_id", "full_name"]
    ]
    repo_data = (
        repo_data.withColumn("license_spdx_id", F.col("license.spdx_id"))
        .withColumn("github_metadata", F.struct(*metadata_cols))
        .select("github_id", "full_name", "license_spdx_id", "github_metadata")
    )
    repo_data = repo_data.withColumn(
        "gh_default_branch",
        F.concat(F.lit("refs/heads/"), F.col("github_metadata.default_branch")),
    )

    all_events = id_names.join(
        watch_events, ["github_id", "full_name"], how="left"
    ).join(fork_events, ["github_id", "full_name"], how="left")
    repo_data = all_events.join(repo_data, ["github_id", "full_name"], how="left")

    return repo_data


def get_swh_origins(spark, path):
    origin_visit_status = read_orc_sanitize_timestamps(spark, path)

    origin_window = Window.partitionBy("origin").orderBy(F.col("visit").desc())

    origins = (
        origin_visit_status.filter(
            F.expr("origin LIKE 'https://github.com/%' AND status = 'full'")
        )
        .withColumn("row_num", F.row_number().over(origin_window))
        .filter(F.col("row_num") == 1)
        .withColumn(
            "full_name",
            F.regexp_extract(F.col("origin"), r"https://github\.com/([^/]+/[^/]+)", 1),
        )
        .selectExpr(
            "origin",
            "date as visit_date",
            "snapshot as snapshot_id",
            "full_name",
        )
    )

    return origins


def get_swh_snapshots(spark, path):
    snapshot_branch = spark.read.orc(path)
    snapshots = (
        snapshot_branch.withColumn("branch_name", F.decode(F.col("name"), "utf-8"))
        .filter(F.col("target_type") == "revision")
        .selectExpr("snapshot_id", "branch_name", "target as revision_id")
    )
    return snapshots


def get_swh_revisions(spark, path):
    revisions = read_orc_sanitize_timestamps(spark, path)

    revisions = revisions.filter(F.expr("type = 'git'")).selectExpr(
        "id as revision_id",
        "date as revision_date",
        "committer_date",
        "directory as directory_id",
    )
    return revisions


def main(spark, args):
    gh_repo_data = get_github_repo_data(spark, args.gharchive_path)
    swh_origins = get_swh_origins(spark, args.swh_origin_path)
    swh_snapshots = get_swh_snapshots(spark, args.swh_snapshot_branch_path)
    swh_revisions = get_swh_revisions(spark, args.swh_revision_path)

    revision_date_window = Window.partitionBy("revision_id").orderBy(
        F.col("revision_date").desc(), F.col("branch_name").desc()
    )
    full_df = (
        swh_origins.join(swh_snapshots, "snapshot_id", how="left")
        .join(
            gh_repo_data.select("full_name", "gh_default_branch"),
            "full_name",
            how="left",
        )
        .filter(
            (F.col("branch_name") == F.col("gh_default_branch"))
            | (
                (F.col("gh_default_branch").isNull())
                & (
                    F.col("branch_name").isin(
                        "HEAD", "refs/heads/master", "refs/heads/main"
                    )
                )
            )
        )
        .drop("gh_default_branch")
        .join(swh_revisions, "revision_id")
        .withColumn("revision_num", F.row_number().over(revision_date_window))
        .filter(F.col("revision_num") == 1)
        .drop("revision_num")
    )

    # merge gharchive events and metadata to swh data
    full_df = full_df.join(gh_repo_data, "full_name", how="left").fillna(
        0, subset=["star_events_count", "fork_events_count"]
    )

    # deduplicate root directories, prioritize earlier commits, older github repos, earlier crawler visits
    directory_window = Window.partitionBy("directory_id").orderBy(
        F.col("license_spdx_id").asc_nulls_last(),
        F.col("star_events_count").desc(),
        F.col("fork_events_count").desc(),
        F.col("revision_date").asc(),
        F.col("github_id").asc(),
        F.col("visit_date").asc(),
    )
    full_df = (
        full_df.withColumn("dir_num", F.row_number().over(directory_window))
        .filter(F.col("dir_num") == 1)
        .drop("dir_num")
    )

    full_df.repartition(512).write.format("parquet").option(
        "compression", "snappy"
    ).mode("overwrite").save(args.output_path)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--gharchive_path",
        default="s3a://bigcode-datasets-us-east-1/gharchive/",
        type=str,
        help="S3 path to GHArchive json files",
    )
    args.add_argument(
        "--swh_origin_path",
        default="s3a://softwareheritage/graph/2023-09-06/orc/origin_visit_status/",
        type=str,
        help="S3 path to SWH origin_visit_status orc files",
    )
    args.add_argument(
        "--swh_snapshot_branch_path",
        default="s3a://softwareheritage/graph/2023-09-06/orc/snapshot_branch/",
        type=str,
        help="S3 path to SWH snapshot_branch orc files",
    )
    args.add_argument(
        "--swh_revision_path",
        default="s3a://softwareheritage/graph/2023-09-06/orc/revision/",
        type=str,
        help="S3 path to SWH revision orc files",
    )
    args.add_argument(
        "--output_path",
        default="s3a://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/repo_data/",
        type=str,
        help="S3 path to save the merged data",
    )
    args = args.parse_args()

    aws_creds = boto3.Session().get_credentials()
    spark = (
        SparkSession.builder.config("spark.sql.shuffle.partitions", 32768)
        .config("spark.hadoop.fs.s3a.access.key", aws_creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_creds.secret_key)
        .config(
            "spark.sql.files.ignoreCorruptFiles",
            "true",  # a couple of GHA files are incomplete
        )
        .appName("merge_gha_swh")
        .getOrCreate()
    )

    main(spark, args)
