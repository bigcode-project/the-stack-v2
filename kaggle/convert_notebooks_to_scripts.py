import argparse

import black
from datasets import load_dataset

from utils.manual_sharding import save_manual_shards
from utils.processing_utils import parse_jupyter_into_script

TEMPLATE = '# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python\nimport numpy as np  # linear algebra\nimport pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)\n\n# Input data files are available in the read-only "../input/" directory\n# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory\nimport os\n\nfor dirname, _, filenames in os.walk("/kaggle/input"):\n    for filename in filenames:\n        print(os.path.join(dirname, filename))\n# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All"\n# You can also write temporary files to /kaggle/temp/, but they won\'t be saved outside of the current session'
SHORT_TEMPLATE = "# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python\n"


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user_name", type=str, default="loubnabnl")
    return parser.parse_args()


def check_syntax(code):
    try:
        compile(code, "<string>", "exec")
        return True
    except Exception as e:
        return False


def format_code(example):
    try:
        # sometimes autopep8 will be stuck, so we need to set a timeout
        formatted_code = black.format_str(example["script"], mode=black.FileMode())
        if formatted_code.startswith(TEMPLATE):
            formatted_code = formatted_code[len(TEMPLATE) :].strip()
        if formatted_code.startswith(SHORT_TEMPLATE):
            formatted_code = formatted_code[len(SHORT_TEMPLATE) :].strip()
        example["script"] = formatted_code
    except Exception as e:
        print(e)
        pass
    return example


def parse_whole_content_kaggle(example):
    notebook = example["content"]
    script_content = parse_jupyter_into_script(notebook, False)
    example["script"] = script_content
    return example


def process_kaggle_jupyter(
    dataset, use_code_execution, workers=1, username="loubnabnl"
):
    init_size = len(dataset)
    dataset = dataset.filter(lambda x: len(x["content"]) <= 500_0000, num_proc=workers)
    dataset = dataset.map(parse_whole_content_kaggle, num_proc=90)
    dataset = dataset.filter(lambda x: len(x["script"]) > 100, num_proc=workers)
    print(
        f"Finish parsing the whole content, total {len(dataset)} notebooks, dropped {100 - len(dataset)/init_size * 100:.2f}% of the original dataset"
    )
    init_size = len(dataset)
    # filter the syntax error
    dataset = dataset.filter(lambda x: check_syntax(x["script"]), num_proc=workers)
    dataset = dataset.map(format_code, num_proc=90, load_from_cache_file=False)
    print(
        f"Check the syntax, total {len(dataset)} notebooks, dropped {100 - len(dataset)/init_size * 100:.2f}% more of the original dataset"
    )
    save_manual_shards(
        dataset,
        user=username,
        remote_dataset_repo="kaggle-scripts-clean",
    )
    print("DONE! Example:\n")
    print(dataset[0]["script"][:100])


if __name__ == "__main__":
    args = get_args()
    dataset = load_dataset("bigcode/kaggle-notebooks-data", split="train")
    process_kaggle_jupyter(
        dataset, use_code_execution=False, workers=36, username=args.user_name
    )
