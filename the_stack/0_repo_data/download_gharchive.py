import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
import s3fs

BUFFER_SIZE = 10 * 1024 * 1024

fs = s3fs.S3FileSystem()
bucket = "bigcode-datasets-us-east-1"
prefix = "gharchive"
existing_files = fs.ls(f"{bucket}/{prefix}")


def download_and_upload(url):
    filename = url.split("/")[-1]
    s3_path = f"{bucket}/{prefix}/{filename}"
    if not filename.endswith(".json.gz"):
        return

    response = requests.head(url)
    response_size = int(response.headers.get("content-length", 0))

    if s3_path in existing_files:
        s3_file_size = fs.info(s3_path).get("size", 0)

        if s3_file_size == response_size:
            return

    response = requests.get(url, stream=True)
    with fs.open(s3_path, "wb") as fout:
        for chunk in response.iter_content(chunk_size=BUFFER_SIZE):
            if chunk:
                fout.write(chunk)


def parse_xml(response):
    root = ET.fromstring(response.text)
    keys = [
        item.text for item in root.iter("{http://doc.s3.amazonaws.com/2006-03-01}Key")
    ]
    next_marker = root.find("{http://doc.s3.amazonaws.com/2006-03-01}NextMarker")
    if hasattr(next_marker, "text"):
        next_marker = next_marker.text
    return keys, next_marker


def get_file_urls(base_url, initial_marker):
    urls = []
    next_marker = initial_marker

    while True:
        response = requests.get(f"{base_url}/?marker={next_marker}")
        keys, next_marker = parse_xml(response)
        urls.extend([f"{base_url}/{key}" for key in keys])
        if next_marker is None:
            break
        print(urls[-1])

    return urls


if __name__ == "__main__":
    base_url = "https://data.gharchive.org"
    initial_marker = "2011-01-01-0.json.gz"
    print("Collecting urls...")
    file_urls = get_file_urls(base_url, initial_marker)
    print("Uploading the files to S3...")
    with ThreadPoolExecutor(max_workers=64) as executor:
        list(tqdm(executor.map(download_and_upload, file_urls), total=len(file_urls)))
