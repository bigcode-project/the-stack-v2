import gzip
import json
from pathlib import Path
from tqdm.auto import tqdm

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

import dask.dataframe as dd
import sqlite3

import datasets

def enumerate_gz_jsonl(file):
    try:
        with gzip.open(file, "r") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except Exception:
                    continue
    except Exception:
        pass
    
    
def pandas_read_parquet_ex(file):
    try:
        df = pd.read_parquet(file)
    except (pa.lib.ArrowNotImplementedError, pa.lib.ArrowInvalid):
        
        # this is a workaround for 'Nested data conversions not implemented for chunked array outputs'
        # seems to work in our particular case but needs a deeper investigation of what is going on
        df = []
        pf = pq.ParquetFile(file)
        for el in pf.iter_batches(use_pandas_metadata=True):
            df.append(el.to_pandas())
        df = pd.concat(df)
        df = df.reset_index(drop=True)
    return df

def df_to_parquet_safe(data, filename):
    filename = Path(filename)
    tmp_file_name = filename.parent / (filename.name + '.__tmp__')
    data.to_parquet(tmp_file_name)
    tmp_file_name.rename(filename)
    

                
def glob_sorted(path, pattern):
    items =  list(Path(path).glob(pattern))
    items = sorted(items)
    return items

def get_opt_outs(src='bigcode-data/opt-out', token=None):
    # Hack from here: https://github.com/huggingface/datasets/issues/1785#issuecomment-1305872400
    datasets.builder.has_sufficient_disk_space = lambda needed_bytes, directory=".": True
    opt_outs = datasets.load_dataset(src, token=token)
    repos_opt_out = []
    users_for_repo_opt_out = []
    users_for_commits_opt_out = []
    users_for_issues_opt_out = []
    for k, v in opt_outs['train'][0].items():
        cond = set(v)
        if 'all' in cond or 'Commits' in cond or 'GitHub issue' in cond:
            if 'all' in cond:
                users_for_repo_opt_out.append(k)
                users_for_commits_opt_out.append(k)
                users_for_issues_opt_out.append(k)
                cond.remove('all')
            else:
                if 'Commits' in cond:
                    users_for_commits_opt_out.append(k)
                    cond.remove('Commits')
                if 'GitHub issue' in cond:
                    users_for_issues_opt_out.append(k)
                    cond.remove('GitHub issue')
        if len(cond) > 0:
            repos_opt_out += list(cond)
    return repos_opt_out, users_for_repo_opt_out, users_for_commits_opt_out, users_for_issues_opt_out


# With adaptation from here https://github.com/bigcode-project/data-curation-pull-request/blob/main/data_analysis/eda.ipynb
# we remove comments from authors in this list
BOT_AUTHORS = set([
    "Apache-HBase",
    "AutorestCI",
    "CLAassistant",
    "cmsbuild",
    "codecov-io",
    "codecov-commenter",
    "coveralls",
    "danger-public",
    "dnfclas",
    "msftclas",
    "PyDocTeur",
    "SparkQA",
    "karma-pr-reporter",
    "danger-public",
    "claassistantio",
    "probot-stale",
])
# we remove comments if author username contains a keyword in this list
BOT_KEYWORDS = ["[bot]", "botmanager", "bors-", "jenkins", "k8s-", "-test-", "travis"]

# we remove comments if author username ends with a suffix in this list
BOT_SUFFIXES = [
    "-automaton",
    "-automation",
    "-benchmark",
    "-build",
    "-deployer",
    "-cloud",
    "bot",
    "-ci",
    "-linter",
    "-teamcity",
    "-test",
    "-testing",
    "-Service-Account",
]

def get_is_user_bot_from_username(un):
    return (
        un is None or 
        un in BOT_AUTHORS or 
        any([kw in un for kw in BOT_KEYWORDS]) or  
        any([un.endswith(suf) for suf in BOT_SUFFIXES])
    )

def get_is_user_bot(event):
    # in the current version of the PR dataset PullRequestReviewEvent
    # and PullRequestReviewCommentEvent have user type info and other 
    # events not, however even former events do not have it always
    if (
        event['type'] == 'PullRequestReviewEvent' or 
        event['type'] == 'PullRequestReviewCommentEvent'
    ):
        if not event['user.type'] is None:
            return event['user.type'].lower() != 'user'
    
    # if actor is present it is always current event source
    # user should be equal to actor just with type field,
    # however for PullRequestEvent user is None,
    # so use actor for user
    # issue events has author instead of actor or user
    if 'actor.login' in event:
        un = event['actor.login']
    elif 'author' in event:
        un = event['author']
    elif 'event_actor_name' in event:
        un = event['event_actor_name']
    else:
        raise RuntimeError(f'unknowns event {event}')

    return get_is_user_bot_from_username(un)

def get_repo_names_licenses(
    bucket='bigcode-datasets',
    key='swh_2023_09_06/stats/repo_licenses/part-00000-474605ad-e5ce-4d86-bf45-acaac7241ba1-c000.snappy.parquet',
    dst=None,
    aws_access_key_id=None,
    aws_secret_access_key=None
):
    import boto3
    dst = Path(dst) / Path(key).name
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.download_file(
        key,
        dst
    )

def repo_names_licenses_convert_to_sqlite(src, dst):
    ddf  = dd.read_parquet(src)
    ddf.to_sql('repo_licenses', f'sqlite:///{dst}')
    con = sqlite3.connect(dst)
    cur = con.cursor()
    res_index = cur.execute("CREATE INDEX idx_repo_name ON repo_licenses (repo_name)")
    con.commit()


    

