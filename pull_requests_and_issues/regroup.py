import hashlib
import ray
from pathlib import Path
import dask.dataframe as dd
import pandas as pd
from pyarrow.parquet import ParquetDataset
import random
import shutil

import polars

import util


def split_items(items, group_size, shuffle=True):
    if shuffle:
        random.shuffle(items)
    items_groups = []
    for i in range(0, len(items), group_size):
        items_groups.append(items[i:i+group_size])
    return items_groups

def hash_column(df, src_column, dst_column):
    df[dst_column] = df[src_column].apply(str).str.encode('utf-8').apply(
        lambda x: hashlib.sha256(x).hexdigest().upper()
    )

def save_group(dst_bucket, data, path, src_rank, bucket_key, row_size_func):
    path = path/f'bucket_{dst_bucket}'
    path.mkdir(parents=True, exist_ok=True)
    
    # seems dask adds __index_level_0__ so remove column with the same name 
    # before saving
    while '__index_level_0__' in data.columns:
        data = data.drop(columns='__index_level_0__')
    file_name = path/f'part_{src_rank}.parquet'
    tmp_file_name = path/f'part_{src_rank}.parquet.__tmp__'
    data.to_parquet(tmp_file_name)
    tmp_file_name.rename(file_name)

    #data['__row_size_for_regroup__'] = data.apply(row_size_func, axis=1)
    #meta_data = data[[bucket_key, '__row_size_for_regroup__']]
    #file_name = path/f'meta_{src_rank}.parquet'
    #tmp_file_name = path/f'meta_{src_rank}.parquet.__tmp__'
    #meta_data.to_parquet(tmp_file_name)
    #tmp_file_name.rename(file_name)

    
def split_df_to_buckets(df, src_rank, dest, bucket_key, bucket_digits_cnt, row_size_func=lambda x: 1):
    dest = Path(dest)
    hash_column(df, bucket_key, 'bucket')
    df['bucket'] = df['bucket'].str[0:bucket_digits_cnt]
    groups = df.groupby('bucket')
    for gr_id, data in groups:
        save_group(gr_id, data, dest, src_rank, bucket_key, row_size_func)
        
def split_df_to_buckets_by_hash(df, src_rank, dest, bucket_key, row_size_func=lambda x: 1):
    dest = Path(dest)
    groups = df.groupby(bucket_key)
    for gr_id, data in groups:
        save_group(gr_id, data, dest, src_rank, bucket_key, row_size_func)

@ray.remote(num_cpus=1)
def split_to_buckets(file, src_rank, dest, bucket_key, bucket_digits_cnt, row_size_func=lambda x: 1):
    df = util.pandas_read_parquet_ex(file)
    split_df_to_buckets(df, src_rank, dest, bucket_key, bucket_digits_cnt, row_size_func)


@ray.remote(num_cpus=1)
def combine_bucket(path):
    dst_filename = path.parent / f'{path.name.split("_")[1]}.parquet'
    dst_filename_tmp = Path(str(dst_filename) + '.__tmp__')

    files = list(path.glob('*.parquet'))
    data = []
    for f in files:
        data.append(pd.read_parquet(f))
    df = pd.concat(data)
    
    df = df.reset_index(drop=True)
    # seems dask adds __index_level_0__ so remove column with the same name 
    # before saving
    while '__index_level_0__' in df.columns:
        df = df.drop(columns='__index_level_0__')
    df.to_parquet(dst_filename_tmp)
    dst_filename_tmp.rename(dst_filename)
    shutil.rmtree(path, ignore_errors=True)

@ray.remote(num_cpus=1)
def ray_shuffle(src, dst, bucket_key, bucket_digits_cnt):
    src = Path(src)
    dst = Path(dst)
    dst.mkdir(parents=True, exist_ok=True)
    
    files = list(src.glob('*.parquet'))
    res = []
    for i, f in enumerate(files):
        res.append(split_to_buckets.remote(f, i, dst, bucket_key, bucket_digits_cnt))
    res = ray.get(res)

    buckets = list(dst.glob('bucket_*'))
    res = []
    for b in buckets:
        res.append(combine_bucket.remote(b))
    res = ray.get(res)
    return res
    

        
