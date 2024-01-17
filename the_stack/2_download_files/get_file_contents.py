import argparse
import gzip
import json
import os
import re
import shutil

import boto3
import cchardet
import enry
import pypeln as pl

from licensedcode.cache import get_licensing
from licensedcode.detection import detect_licenses
from tqdm.auto import tqdm

# License file regexes taken from
# https://github.com/go-enry/go-license-detector/blob/master/licensedb/internal/investigation.go
license_file_names = [
    "li[cs]en[cs]e(s?)",
    "legal",
    "copy(left|right|ing)",
    "unlicense",
    "[al]?gpl([-_ v]?)(\d\.?\d?)?",  # AGPLv3
    "bsd(l?)",  # BSDL
    "mit(x?)",  # MITX
    "apache",
    "artistic",  # Artistic.txt
    "copying(v?)(\d?)",  # COPYING3, COPYINGv3
    "disclaimer",
    "eupl",
    "gfdl",
    "[cm]pl",
    "cc0",
    "al([-_ v]?)(\d\.?\d)?",  # AL2.0
    "about",
    "notice",
    "readme",
    "guidelines",
]

license_file_re = re.compile(
    rf"^(|.*[-_. ])({'|'.join(license_file_names)})(|[-_. ].*)$", re.IGNORECASE
)


def on_start():
    return dict(s3=boto3.client("s3"))


def list_files(s3, bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for content in response.get("Contents", []):
        if content["Key"].endswith(".json.gz"):
            yield content["Key"]


def read_records(s3, bucket, file):
    try:
        obj = s3.get_object(Bucket=bucket, Key=file)
        for line in gzip.GzipFile(None, "r", fileobj=obj["Body"]):
            record = json.loads(line)
            record["content"] = None
            record["src_encoding"] = None
            record["language"] = None
            record["is_vendor"] = False
            record["is_generated"] = False
            record["licenses"] = []

            if record["length"] < 10 * 1024 * 1024:
                yield record
    except Exception as e:
        # S3 connection errors, restart the job to reprocess the file
        print(file, e)


def get_blob(record, s3):
    try:
        blob = s3.get_object(
            Bucket="softwareheritage", Key=f"content/{record['blob_id']}"
        )
        content = gzip.GzipFile(None, "rb", fileobj=blob["Body"]).read()
    except gzip.BadGzipFile:
        content = None
    except Exception as e:
        # e.g. no such key
        print(record["blob_id"], e)
        content = None
    if content is None or b"\0" in content:
        record["content"] = None
    else:
        record["content"] = content
    return record


def decode_content(content_bytes):
    charset = "UTF-8"
    try:
        text = content_bytes.decode(charset)
    except UnicodeDecodeError:
        encoding_det = cchardet.detect(content_bytes)["encoding"]
        if not encoding_det or encoding_det == charset:
            return None, None
        charset = encoding_det

        try:
            text = content_bytes.decode(charset)
        except (UnicodeDecodeError, LookupError):
            return None, None
    return text, charset


def get_lang_license(record) -> str:
    if record["content"] is None:
        return json.dumps(record)

    filename = record["filenames"][0]  # TODO: better filename heuristic?
    # Detect language and file labels
    record["language"] = enry.get_language(filename, record["content"])
    record["language"] = None if record["language"] == "" else record["language"]
    record["is_vendor"] = enry.is_vendor(filename)
    record["is_generated"] = enry.is_generated(filename, record["content"])

    record["content"], record["src_encoding"] = decode_content(record["content"])

    if record["content"] is None:
        return json.dumps(record)

    # If the file is a license file, try to detect the license
    if any(license_file_re.match(filename) for filename in record["filenames"]):
        detected_licenses = set()

        for lic in detect_licenses(query_string=record["content"], deadline=100):
            licensing = get_licensing()
            symbols = licensing.license_symbols(lic.license_expression)
            for sym in symbols:
                detected_licenses.add(sym.key)
        record["licenses"] = list(detected_licenses)

    if len(record["licenses"]) == 0 and record["language"] is None:
        # save some space if the file is not interesting
        record["content"] = None

    return json.dumps(record)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument(
        "--blob_prefix",
        required=True,
        type=int,
        help="Partitioning prefix of blob_id. Will be turned to hex, e.g. 255->ff",
    )
    args.add_argument(
        "--input_path",
        help="S3 path to the list of files to download",
        default="s3://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/files_to_download",
    )
    args.add_argument(
        "--output_path",
        help="S3 path to save the file contents",
        default="s3://bigcode-datasets-us-east-1/the_stack/swh_2023_09_06/file_contents",
    )

    args = args.parse_args()

    num_cores = int(os.environ["SLURM_CPUS_PER_TASK"])
    blob_prefix = f"{args.blob_prefix:0{2}x}"  # int to hex

    s3 = boto3.client("s3")
    input_bucket = args.input_files.split("/")[2]
    input_prefix = "/".join(args.input_files.split("/")[3:])
    files = list_files(
        s3,
        input_bucket,
        f"{input_prefix}/blob_prefix={blob_prefix}/",
    )

    cache_dir = f"/scratch/stackv2/file_contents/blob_prefix={blob_prefix}/"
    shutil.rmtree(cache_dir, ignore_errors=True)
    os.makedirs(cache_dir, exist_ok=True)

    for file in files:
        print(file)
        file_name = file.split("/")[-1]
        output_bucket = args.output_path.split("/")[2]
        output_prefix = "/".join(args.output_path.split("/")[3:])
        if s3.list_objects_v2(
            Bucket=output_bucket,
            Prefix=f"{output_prefix}/blob_prefix={blob_prefix}/{file_name}",
        ).get("Contents", []):
            # file was already processed
            continue

        inputs = read_records(s3, input_bucket, file)
        pipe = pl.process.map(
            get_blob,
            inputs,
            workers=num_cores // 2,
            maxsize=500,
            on_start=on_start,
            timeout=60,
        )
        pipe = pl.process.map(
            get_lang_license, pipe, workers=num_cores // 2, maxsize=500, timeout=120
        )

        local_file_path = os.path.join(cache_dir, file_name)

        with gzip.open(local_file_path, "wt") as fout:
            for record in tqdm(pipe):
                fout.write(record + "\n")
            fout.flush()

        s3.upload_file(
            local_file_path,
            output_bucket,
            f"{output_prefix}/blob_prefix={blob_prefix}/{file_name}",
        )
        os.remove(local_file_path)

    shutil.rmtree(cache_dir)
    print("DONE")
