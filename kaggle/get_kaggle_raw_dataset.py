# code for converting the kaggle dataset to standard dataframe (metadata not added here, see retrieve_metadata.py)
import json
from multiprocessing import Pool, cpu_count
from pathlib import Path

import pandas as pd
from datasets import Dataset

# dataset: https://www.kaggle.com/datasets/kaggle/meta-kaggle-code
code_base_path = Path("./kaggle_data/kaggle-code-data/data")


# Function to extract content from a notebook
def extract_content(fp):
    try:
        with open(fp, "r", encoding="utf-8") as f:
            content = json.load(f)
            cells = content.get("cells", [])
            cells = json.dumps(cells)
            file_id = fp.stem
            return {"file_id": file_id, "content": cells, "local_path": str(fp)}
    except json.JSONDecodeError:
        print(f"Error decoding JSON for file: {fp}")
        return {"file_id": None, "content": None, "local_path": str(fp)}


def find_notebooks(base_dir):
    return list(base_dir.glob("*/*.ipynb"))


def main():
    sub_dirs = [x for x in code_base_path.iterdir() if x.is_dir()]

    # Use a Pool of workers to find notebooks
    with Pool(cpu_count()) as p:
        notebook_lists = p.map(find_notebooks, sub_dirs)
    print(f"number of notebook dirs retrieved {len(notebook_lists)}")
    # Flatten the list of lists
    all_notebooks = [item for sublist in notebook_lists for item in sublist]
    print(f"total number of notebooks {len(all_notebooks)}")

    # Use a Pool of workers to extract content
    print("starting extraction...")
    with Pool(cpu_count()) as p:
        data = p.map(extract_content, all_notebooks)
    print("extraction finished")

    # save data
    df = pd.DataFrame(data)
    df.to_csv("kaggle_notebooks.csv", index=False)
    print("saved to csv file")
    ds = Dataset.from_pandas(df)
    # filter out None values
    ds = ds.filter(lambda x: x["file_id"] is not None)
    print(f"number of notebooks after filtering {len(ds)}")
    ds.push_to_hub("kaggle-notebooks-data")
    print("pushed to hub")
    return ds


if __name__ == "__main__":
    main()
