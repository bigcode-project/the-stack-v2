import re
import pandas as pd
from pathlib import Path
import util
import ray
from functools import partial
import numpy as np

def add_license_to_pr_remove_non_permissive(dfis, repo_licenses_sqlite_file):
    
    repos = list(dfis['repo_name'].unique())
   
    con = sqlite3.connect(f'file:{repo_licenses_sqlite_file}?mode=ro', uri=True)
    res = con.execute(
        f"SELECT repo_name, license_type FROM repo_licenses WHERE repo_name in ({','.join(['?']*len(repos))})",
        repos
    )
    res = res.fetchall()
    dfrl = pd.DataFrame(res, columns=['repo_name', 'license_type'])
    
    dfis = dfis.merge(dfrl, on='repo_name', how='right')
    dfis = dfis[dfis['license_type'] != 'non_permissive']
    return dfis

def combine_commit_data_leave_only_diffs(row):
    hf = row['head_files']
    if hf is None:
        hf = []
    elif hf is dict:
        hf = [hf]
    else:
        hf = list(hf)
    bf = row['base_files']
    if bf is None:
        bf = []
    elif bf is dict:
        bf = [bf]
    else:
        bf = list(bf)
    data = defaultdict(dict)
    for el in bf:
        data[el['path']]['base'] = el
    for el in hf:
        data[el['path']]['head'] = el

    dr = []
    num_diff_files = 0
    num_base_only_files = 0
    num_head_only_files = 0
    for val in data.values():
        if 'head' in val and 'base' in val:
            num_diff_files += 1
        else:
            continue
        r = {
            'head_blob_id': val['head']['blob_id'] if 'head' in val else None,
            'head_content': val['head']['content'] if 'head' in val else None,
            'head_is_generated': val['head']['is_generated'] if 'head' in val else None,
            'head_is_vendor': val['head']['is_vendor'] if 'head' in val else None,
            'head_language': val['head']['language'] if 'head' in val else None,
            'head_length': val['head']['length'] if 'head' in val else None,
            'head_path': val['head']['path'] if 'head' in val else None,
            'base_blob_id': val['base']['blob_id'] if 'base' in val else None,
            'base_content': val['base']['content'] if 'base' in val else None,
            'base_is_generated': val['base']['is_generated'] if 'base' in val else None,
            'base_is_vendor': val['base']['is_vendor'] if 'base' in val else None,
            'base_language': val['base']['language'] if 'base' in val else None,
            'base_length': val['base']['length'] if 'base' in val else None,
            'base_path': val['base']['path'] if 'base' in val else None,
        }
        dr.append(r)
        
    return {
        'head_base_files': dr,
        'num_diff_files': num_diff_files,
    }


def group_base_head_files_leave_only_diffs(df):
    if 'head_base_files' in df.columns:
        return df
    df = df.reset_index(drop=True)
    df_hbf = pd.DataFrame(list(df.apply(combine_commit_data_leave_only_diffs, axis=1)))
    df = pd.concat([df, df_hbf], axis=1)
    df = df[[
        'repo_name','pull_request.guid',
        'head_directory_id', 'base_directory_id', 'head_commit_id', 'base_commit_id',
        'head_base_files', 'num_diff_files', 
        'revision_date', 'committer_date', 'author',
        'message'
    ]]
    df = df[df['num_diff_files'] > 0]
    return df


def remove_opt_outs(df, repos_opt_out, users_for_repo_opt_out):
    df = df[df['repo_name'].isin(repos_opt_out) == False]
    df['username'] = df['repo_name'].str.split('/', expand=True)[0]
    df = df[df['username'].isin(users_for_repo_opt_out) == False]
    df = df.drop(columns='username')
    return df

def count_range_of_chanes_remove_too_big_row(row, max_changes_length):
    ttl_sz = 0
    for example in row['head_base_files']:
        diff_info, base_content = get_line_diff_range(example)
        example['base_change_start'] = diff_info['old_change_start']
        example['base_change_end'] = diff_info['old_change_end']
        sz = sum(len(el) for el in base_content[
             diff_info['old_change_start']:
             diff_info['old_change_end']
        ])
        ttl_sz += sz
        if ttl_sz > max_changes_length:
            row['head_base_files'] = None
            return row
    return row
            
        
def count_range_of_chanes_remove_too_big(df, max_changes_length):
    df = df.apply(
        partial(count_range_of_chanes_remove_too_big_row, max_changes_length=max_changes_length),
        axis=1
    )
    df = df[df['head_base_files'].isna() == False]
    return df

@ray.remote
def filter_nonpermissive_opt_outs_and_prepare_commit_pairs(
    file, dst, repos_opt_out, users_for_repo_opt_out, max_changes_length, repo_licenses_sqlite_file
):
    dst = Path(dst)

    dst_file = dst / file.name
    if dst_file.is_file():
        return 1

    stats = {}
    dfis = pd.read_parquet(file)
    stats['original'] = len(dfis)
    dfis = add_license_to_pr_remove_non_permissive(dfis, repo_licenses_sqlite_file)
    stats['removed_non_permissive'] = len(dfis)
    dfis = remove_opt_outs(dfis, repos_opt_out, users_for_repo_opt_out)
    stats['removed_opt_outs'] = len(dfis)
    dfis = group_base_head_files_leave_only_diffs(dfis)
    stats['removed_non_diffs'] = len(dfis)
    dfis = count_range_of_chanes_remove_too_big(dfis, max_changes_length)
    stats['removed_too_big'] = len(dfis)
    
    util.df_to_parquet_safe(dfis, dst_file)
    return stats


    

class FilterParams:
    min_alphanum_fraction = .25
    max_hex_fraction = .25
    max_num_lines = 100000
    max_avg_line_length = 100
    max_max_line_length = 1000

    # form here https://huggingface.co/datasets/bigcode-data/stackv2_smol_sample_50k
    in_langs = None
    dependency_files = None
    doc_langs = set(["Markdown", "reStructuredText", "RMarkdown", "RDoc", "AsciiDoc"])

    english_lang_pattern = re.compile(r"\s+(the|be|to|of|and|that|have|with)\s+")
    
def get_content_stats(content):
    if len(content) == 0:
        return {
            'alphanum_fraction' : 0,
            'alpha_fraction' : 0,
            'hex_fraction' : 0,
            'num_lines' : 0,
            'avg_line_length' : 0,
            'max_line_length' : 0,
        }
    content_line_lengths = [len(el) for el in content.splitlines()]
    return {
        'alphanum_fraction' : np.mean([c.isalnum() for c in content]),
        'alpha_fraction' : np.mean([c.isalpha() for c in content]),
        'hex_fraction' : (content.count("\\x") * 2) / len(content),
        'num_lines' : len(content_line_lengths),
        'avg_line_length' : np.mean(content_line_lengths),
        'max_line_length' : np.max(content_line_lengths),
    }
    
def is_valid_file(file, filter_params):
    content_stats = get_content_stats(file['base_content'])
    return (
        content_stats['alphanum_fraction'] > filter_params.min_alphanum_fraction and
        content_stats['hex_fraction'] < filter_params.max_hex_fraction and
        content_stats['num_lines'] < filter_params.max_num_lines and
        (
            filter_params.in_langs is None or
            filter_params.dependency_files is None or 
            file['base_language'] in filter_params.in_langs or
            Path(file['base_path']).name in filter_params.dependency_files
        ) and
        (
            file['base_language'] in filter_params.doc_langs or
            (
                content_stats['avg_line_length'] < filter_params.max_avg_line_length and
                content_stats['max_line_length'] < filter_params.max_max_line_length      
            )
        ) and
        # remove Markdown docs that don't contain English words in the content
        (
            file['base_language'] != "Markdown" or
            filter_params.english_lang_pattern.search(file['base_content'])
        )
    )

def clean_files(files, filter_params):
    files_indexes = [
        i
        for i, file in enumerate(files) 
        if is_valid_file(
            file,
            filter_params
        )]
    if len(files_indexes) == 0:
        return None
    return files[files_indexes]

@ray.remote
def clean_files_bucket(file, dst, filter_params):
    dst = Path(dst)
    file = Path(file)
    dst = dst / file.name
    if dst.is_file():
        return 1
    df = pd.read_parquet(file)
    df['head_base_files'] = df['head_base_files'].apply(partial(clean_files, filter_params=filter_params))
    df = df.dropna(subset='head_base_files')
    util.df_to_parquet_safe(df, dst)

