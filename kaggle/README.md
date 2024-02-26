# Kaggle Notebooks

Here you can find the code for:
- Processing the [Meta Kaggle Code](https://www.kaggle.com/datasets/kaggle/meta-kaggle-code) dataset to convert notebooks into python scripts
- Downloading and retrieving some metadata on the datasets used in each notebook when available (`df.info()`, column names and 4 examples).

## Getting started
```
pip install -r requirements.txt
mkdir -p kaggle_data/metadata_kaggle
mkdir -p kaggle_data/kaggle-code-data
```

- Download metadata tables from https://www.kaggle.com/datasets/kaggle/meta-kaggle at ./kaggle_data/metadata_kaggle/
- Download notebooks dataset from https://www.kaggle.com/datasets/kaggle/meta-kaggle-code at ./kaggle_data/kaggle-code-data/data (note: it can take many hours)

Some mapping between the notebooks dataset `meta-kaggle-code` and the csv tables available at `meta-kaggle` needs to be done to retrieve the metadata of each notebook, in particular we want to find the dataset name (owner/data_name) to download datasets using kaggle API so we can add information about the dataset used in each notebooks. We also want to add upvotes, title, data description and competition description/title and any other relevant information...

## Convert Kaggle dataset to a dataframe

The notebooks are stored in `.ipynb`  files, to store them in an HF dataset on the hub called `kaggle-notebooks-data` use:

```bash
python get_kaggle_raw_dataset.py
```

## Convert the notebooks to scripts

To convert the notebooks in your dataset to scripts (Python & R) use:
```bash
python convert_notebooks_to_scripts.py
```

This removed 20% of the files due to parsing and syntax errors. We also run deduplication on the dataset using code from [bigcode-dataset/near_deduplication](https://github.com/bigcode-project/bigcode-dataset/blob/main/near_deduplication), 78% of the files are removed because they were duplicates. The remaining dataset has 580k notebooks.

## Add metadata
The dataset only has the notebooks content now, to add metadata such as the names of the datasets used in the notebooks, you need to match the notebook IDs to the csv files you downloaded in `Getting Started`.

```bash
python retrieve_metadata.py
```

## Add Kaggle datasets information
In teh metadata we retrived, we can sometimes find the description of the datasets used in the notebooks. In this section we want to add more information on these datasets such as the column names, some examples and `df.info()` to get more context on what's happening in the notebook. To do that we download the datasets used in the notebooks using Kaggle API.

Login to Kaggle with your API Key (see [documentation](https://www.kaggle.com/docs/api)) and run:
```bash
python retreive_dataset.py --total-slices 4000 --ith-slice 0 --get-datainfo --download-data
```

You can find the final Kaggle dataset where we added relevant metadata at [starcoderdata_extras](https://huggingface.co/datasets/HuggingFaceTB/starcoderdata_extras).