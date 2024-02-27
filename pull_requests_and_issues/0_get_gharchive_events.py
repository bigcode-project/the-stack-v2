import time
from datetime import timedelta
from urllib import request
from tqdm.auto import tqdm

from cfg import gharchives_path, edate, sdate

def run():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0"
    }
    prefix_url = "https://data.gharchive.org/"

    delta = edate - sdate  # as timedelta

    gharchives_path.mkdir(parents=True, exist_ok=True)

    for i in tqdm(range(delta.days + 1)):
        day = sdate + timedelta(days=i)
        for j in range(24):
            file_name = f"{day}-{j}.json.gz"
            save_path = gharchives_path / file_name
            if save_path.is_file():
                continue

            # TODO: add port management as they seems to run out time after time:
            #   <urlopen error [Errno 99] Cannot assign requested address>
            try:
                url = prefix_url + file_name
                req = request.Request(url=url, headers=headers)
                output = request.urlopen(req).read()

                with open(save_path, "wb") as f:
                    f.write(output)
            except Exception as e:
                print("Can not download the following url..")
                print(url)
                print("-----------------------------------------")
                print(e)
                print("-----------------------------------------\n")

            time.sleep(1)

if __name__ == "__main__":
    run()
    
