from pathlib import Path
import pandas as pd
import json
from tqdm.auto import tqdm
import matplotlib.pyplot as plt
from collections import defaultdict
from pyarrow.parquet import ParquetFile
import numpy as np
import polars as pl
from difflib import SequenceMatcher
import datasets
import os
import util
import regroup
from functools import cmp_to_key
import sqlite3
import itertools
import pyarrow as pa
import sys
import ray
import re

import process_pr_events as ppre

class RenderError(ValueError):
    pass

class RenderParams:
    min_title_size = 10
    max_title_size = 500
    
    max_text_lines_range = 80
    min_text_size = 20
    max_text_size = 1000

    code_comment_diff_hunk_full_code_prob = 1.0
    code_comment_diff_hunk_min_extra_lines_range = 3
    code_comment_diff_hunk_max_extra_lines_range = 32
    code_comment_max_diff_hunk_size = 10000

    base_code_full_code_prob = .2
    base_code_extra_lines_range = 32
    
    commit_diff_hunk_min_extra_lines_range = 3
    commit_diff_hunk_max_extra_lines_range = 10
    join_commit_diff_hunks = True

    min_pr_length = 200
    max_pr_length = 1000000

    randomness_on = True
    subsample_pr_per_repo = True
    strip_automated_email_text = True
    anonymise_usernames = True

    subsample_langs = {
        'YAML': {
            'all_base_file_ratio': .5,
            'retain_prob': .1, 
            'subsample_keys': ['pack', 'lock', 'yarn', 'output', 'swagger', 'openapi'],
        },
        'JSON': {
            'all_base_file_ratio': .5,
            'retain_prob': .1, 
            'subsample_keys': ['pack', 'lock', 'output'],
        },
    }    

PULL_REQUEST_EVENT_TOKEN = '<pr>'
PULL_REQUEST_STATUS = '<pr_status>'
PULL_REQUEST_REPO_NAME_TOKEN = '<repo_name>'
PULL_REQUEST_IS_MERGED_TOKEN = '<pr_is_merged>'

BASE_TOKEN = '<pr_base>'
FILE_PATH_TOKEN = '<pr_file>'
CODE_TOKEN = '<pr_base_code>'

PULL_REQUEST_DIFF_TOKEN = '<pr_diff>'
DIFF_HUNK_TOKEN = '<pr_diff_hunk>'

GENERAL_COMMENT_TOKEN = "<pr_comment>"
PULL_REQUEST_EVENT_ID_TOKEN = '<pr_event_id>'

PULL_REQUEST_REVIEW_TOKEN = '<pr_review>'
PULL_REQUEST_REVIEW_STATE_TOKEN = '<pr_review_state>'

PULL_REQUEST_REVIEW_COMMENT_TOKEN = '<pr_review_comment>'
COMMENT_IN_REPLY_TO_REVIEW_ID_TOKEN = '<pr_in_reply_to_review_id>'
COMMENT_IN_REPLY_TO_COMMENT_ID_TOKEN = '<pr_in_reply_to_comment_id>'
DIFF_HUNK_COMMENT_LINE_TOKEN = '<pr_diff_hunk_comment_line>'
TEXT_TOKEN = "<pr_comment>"

ALL_TOKENS_RE = '|'.join([
    PULL_REQUEST_EVENT_TOKEN,
    PULL_REQUEST_STATUS,
    PULL_REQUEST_REPO_NAME_TOKEN,
    PULL_REQUEST_IS_MERGED_TOKEN,
    BASE_TOKEN,
    FILE_PATH_TOKEN,
    CODE_TOKEN,
    PULL_REQUEST_DIFF_TOKEN,
    DIFF_HUNK_TOKEN,
    GENERAL_COMMENT_TOKEN,
    PULL_REQUEST_EVENT_ID_TOKEN,
    PULL_REQUEST_REVIEW_TOKEN,
    PULL_REQUEST_REVIEW_STATE_TOKEN,
    PULL_REQUEST_REVIEW_COMMENT_TOKEN,
    COMMENT_IN_REPLY_TO_REVIEW_ID_TOKEN,
    COMMENT_IN_REPLY_TO_COMMENT_ID_TOKEN,
    DIFF_HUNK_COMMENT_LINE_TOKEN,
    TEXT_TOKEN
])

TITLE_PLACEHOLDER = 'Title: '
TEXT_TRUNCATION_PLACEHOLDER = '[TRUNCATED]'
TEXT_OMITTED_PLACEHOLDER = '[OMITTED]'
DIFF_OMITTED_PLACEHOLDER = '[DIFFS OMITTED]'


def truncate_long_text(text, render_params):
    lines = text.split("\n")
    nb_lines = len(lines)
    if nb_lines > render_params.max_text_lines_range:
        text = "\n".join(lines[: render_params.max_text_lines_range - 20]) + f"\n{TEXT_TRUNCATION_PLACEHOLDER}\n"
        text += "\n".join(lines[-20:])
    return text

def remove_markdown_comments(text):
    return ''.join(el.rsplit('-->', 1)[-1] for el in text.split('<!--'))

def render_title(text, render_params):
    if text is None:
        text = ''

    res = truncate_long_text(text, render_params)

    if len(res) < render_params.min_title_size:
        raise RenderError('min_text_size')

    textl = text.lower()
    if (
        'dependencies' in textl or
        'dependency' in textl or
        'depend' in textl or
        'release' in textl
    ):
        raise RenderError('keyword error')

    if len(res) > render_params.max_title_size:
        res = res[:render_params.max_title_size]

    # Title is renderd only in PR open or edited and follows by new line all the time
    return TITLE_PLACEHOLDER + res + '\n'

def render_text(user, text, render_params, ignore_min=False):
    if text is None:
        text = user

    if render_params.strip_automated_email_text:
        text = ppre._strip_automated_email_text(text)

    if 'Qwiet' in text:
        raise RenderError()
    if 'codecov' in text.lower():
        return user + ': ' + TEXT_OMITTED_PLACEHOLDER + 'CodeCov'

    text = remove_markdown_comments(text)

    res = truncate_long_text(text, render_params)

    if len(res) < render_params.min_text_size and not ignore_min:
        raise RenderError('min_text_size')

    if len(res) > render_params.max_text_size:
        res = res[:render_params.max_text_size]

    if len(res) == 0:
        return user

    return user + ': ' + res

def render_issue(data, render_params, c_length):
    res = ''
    if data['type'] == 'comment':
        try:
            res += f"{GENERAL_COMMENT_TOKEN}{render_text(data['actor.login'], data['text'], render_params)}"
            if not data['comment_id'] is None:
                res += f"{PULL_REQUEST_EVENT_ID_TOKEN}{int(data['comment_id'])}"
            # TODO: decide if to remove this
            if not data['action'] is None:
                res += f"{PULL_REQUEST_STATUS}{data['action'].lower()}"
        except RenderError:
            return ''
    elif data['type'] == 'issue':
        print(data)
        # do not render these events
        pass
    return res

def render_pr_event(data, is_pr_open_rendered, render_params, c_length):
    res = PULL_REQUEST_EVENT_TOKEN
    try:
        if data['action'] == 'edited' or data['action'] == 'opened' or data['action'] == 'reopened':
            res += render_title(data['title'], render_params)
            res += render_text(data['actor.login'], data['text'], render_params)
        else:
            res += data['actor.login']
        if not data['action'] is None:
            res += f"{PULL_REQUEST_STATUS}{data['action'].lower()}"
        if not is_pr_open_rendered:
            res += f"{PULL_REQUEST_REPO_NAME_TOKEN}{data['base_repo_name']}"
        if data['action'] == 'closed':
            res += f"{PULL_REQUEST_IS_MERGED_TOKEN}{data['pull_request.merged']}"
    except RenderError:
        # if title or text have been filtered out on open event remove the PR
        if data['action'] == 'opened':
            raise
        # remove event otherwise
        return ''
    return res

def update_start_line(header, skipped_lines):
    try:
        header = header.split('@@')
        lines =  [el.strip().split(',') for el in header[1].strip(' -').split('+')]
        lines[0][0] = int(lines[0][0]) - skipped_lines
        lines[1][0] = int(lines[1][0]) - skipped_lines

        assert lines[0][0] >= 0
        assert lines[1][0] >= 0

        if len(lines[0]) == 1:
            lines[0].append('')
        else:
            lines[0][1] = ','+lines[0][1]
        if len(lines[1]) == 1:
            lines[1].append('')
        else:
            lines[1][1] = ','+lines[1][1]

        return f'@@ -{lines[0][0]}{lines[0][1]} +{lines[1][0]}{lines[1][1]} @@{header[2]}'
    except Exception:
        raise

# from here https://huggingface.co/spaces/bigcode/code_review_demo/blob/main/app.py
def render_code_comment_diff_hunk(diff_hunk, position, skipped_lines, render_params):
    lines = diff_hunk.split('\n')
    # sometimes diff hunks is empty or otherwise do not have heade, but still have comment for the file 
    if '@@' in lines[0]:
        lines[0] = update_start_line(lines[0], skipped_lines)
    start_line = 0
    end_line = len(lines)
    # preserve the first line with the @@ notation
    if render_params.randomness_on and np.random.random() >= render_params.code_comment_diff_hunk_full_code_prob:
        start_offset = np.random.randint(
            render_params.code_comment_diff_hunk_min_extra_lines_range,
            render_params.code_comment_diff_hunk_max_extra_lines_range
        )
        end_offset = np.random.randint(
            render_params.code_comment_diff_hunk_min_extra_lines_range,
            render_params.code_comment_diff_hunk_max_extra_lines_range
        )
        lines = diff_hunk.split('\n')
        start_line = max(int(position) - start_offset - 1, 0)
        end_line = min(int(position) + end_offset, len(lines))
    diff_hunk = lines[0] + '\n' + '\n'.join(lines[start_line + 1:end_line + 1])

    if len(diff_hunk) > render_params.code_comment_max_diff_hunk_size:
        return DIFF_HUNK_TOKEN + TEXT_OMITTED_PLACEHOLDER
    return DIFF_HUNK_TOKEN + diff_hunk

def render_pr_review_event(data, base_info, render_params, c_length):
    res = ''
    try:
        if data['type'] == 'PullRequestReviewEvent':
            res += PULL_REQUEST_REVIEW_TOKEN
            if data['text']:
                res += render_text(data['actor.login'], data['text'], render_params, ignore_min=True)
            else:
                res += data['actor.login']
            res += f"{PULL_REQUEST_EVENT_ID_TOKEN}{int(data['review_comment.id'])}"
            res += f"{PULL_REQUEST_REVIEW_STATE_TOKEN}{data['review.state']}"
        else:
            res += PULL_REQUEST_REVIEW_COMMENT_TOKEN
            res += f"{PULL_REQUEST_EVENT_ID_TOKEN}{int(data['review_comment.id'])}"
            if not data['comment.pull_request_review_id'] is None:
                res += f"{COMMENT_IN_REPLY_TO_REVIEW_ID_TOKEN}{int(data['comment.pull_request_review_id'])}"
            if not data['comment.in_reply_to_id'] is None:
                res += f"{COMMENT_IN_REPLY_TO_COMMENT_ID_TOKEN}{int(data['comment.in_reply_to_id'])}"
            res += f"{FILE_PATH_TOKEN}{data['comment.path']}"
            res += f"{DIFF_HUNK_COMMENT_LINE_TOKEN}{int(data['comment.original_position'])}"
            skipped_lines = (
                base_info[data['comment.path']]['old_change_start'] 
                if  data['comment.path'] in base_info else 0
            )
            res += render_code_comment_diff_hunk(
                data['comment.diff_hunk'],
                data['comment.original_position'],
                skipped_lines,
                render_params
            )
            res += TEXT_TOKEN + render_text(data['actor.login'], data['text'], render_params)
    except RenderError:
        # remove event if text is filtered out
        return ''

    return res

# from https://github.com/bigcode-project/bigcode-dataset/blob/main/preprocessing/filtering_git_commits.ipynb
def get_line_diff_range(example):
    old_file_start = sys.maxsize
    old_file_end = 0

    new_file_start = sys.maxsize
    new_file_end = 0
    
    n_inserts = 0
    n_deletes = 0

    base_content = example["base_content"].splitlines()
    
    for group in SequenceMatcher(
        None,
        base_content,
        example["head_content"].splitlines()
    ).get_grouped_opcodes():
        group = [g for g in group if g[0]!="equal"]
        
        for element in group:
            if element[0]=="insert":
                n_inserts += element[4]-element[3]
            if element[0]=="delete":
                n_deletes += element[2]-element[1] 
            if element[0]=="replace":
                n_deletes += element[2]-element[1]
                n_inserts += element[4]-element[3]

        first, last = group[0], group[-1]
        file1_range = (first[1], last[2])
        file2_range = (first[3], last[4])
        
        old_file_start = min(file1_range[0], old_file_start)
        old_file_end = max(file1_range[1], old_file_end)

        new_file_start = min(file2_range[0], new_file_start)
        new_file_end = max(file2_range[1], new_file_end)


    # -2 for compatibility with gh_diff
    res = {}
    res["old_change_start"] = old_file_start
    res["old_change_end"] = old_file_end
    res["old_change_range"] = old_file_end - old_file_start

    res["new_change_start"] = new_file_start
    res["new_change_end"] = new_file_end
    res["new_change_range"] = new_file_end - new_file_start
    
    res["n_inserts"] = n_inserts
    res["n_deletes"] = n_deletes
    res["n_changes"] = n_inserts + n_deletes
    
    # we return base contetn spllit to lines as optimisation not to do it again
    return res, base_content



def prepare_base(base_info, render_params):
    if render_params.randomness_on and np.random.random() < render_params.base_code_full_code_prob:
        base_info['old_change_start'] = 0
        base_info['old_change_end'] = len(base_info['base_content'])
    else:
        if render_params.randomness_on:
            start_offset = np.random.randint(0, render_params.base_code_extra_lines_range)
            end_offset = np.random.randint(0, render_params.base_code_extra_lines_range)
        else:
            start_offset = render_params.base_code_extra_lines_range // 2
            end_offset = render_params.base_code_extra_lines_range // 2

        old_lines = base_info['base_content'].splitlines()
        old_start = max(0, base_info['old_change_start']- start_offset)
        old_end = min(len(old_lines), base_info['old_change_end'] + end_offset)
        code_base = "\n".join(old_lines[old_start:old_end])
        base_info['old_change_start'] = old_start
        base_info['old_change_end'] = old_end
        base_info['base_content'] = code_base

def get_if_to_remove_file_by_subsample(path, file, render_params, ttl_sz):
    if not file['base_language'] in render_params.subsample_langs:
        return False
    sub_info = render_params.subsample_langs[file['base_language']]
    if ttl_sz < 1:
        ttl_sz = 1
    subsample = (
        any(key in path for key in sub_info['subsample_keys']) or
        len(file['base_content']) / ttl_sz > sub_info['all_base_file_ratio']
    )
    if subsample:
        return np.random.random() > sub_info['retain_prob']
    return False

def render_base_files(diffs, render_params, c_length):
    files = defaultdict(lambda : {
        'old_change_start': sys.maxsize,
        'old_change_end': 0,
        'base_content': None,
        'size': 0,
        'base_language': None
    })
    for event in diffs:
        for example in event['head_base_files']:
            if example["base_content"] is None or example["head_content"] is None:
                continue
            diff_info, base_content_lines = get_line_diff_range(example)
            changed_start_stop = False
            if diff_info['old_change_start'] < files[example['base_path']]['old_change_start']:
                files[example['base_path']]['old_change_start'] = diff_info['old_change_start']
                changed_start_stop = True
            if diff_info['old_change_end'] > files[example['base_path']]['old_change_end']:
                files[example['base_path']]['old_change_end'] = diff_info['old_change_end']
                changed_start_stop = True
            if files[example['base_path']]['base_content'] is None:
                files[example['base_path']]['base_content'] = example['base_content']
                files[example['base_path']]['base_path'] = example['base_path']
                files[example['base_path']]['base_language'] = (
                    Path(example['base_path']).name if example['base_language'] is None else example['base_language']
                )
            if changed_start_stop:
                # optimization to stop computing change ranges early if base files ranges become too big
                files[example['base_path']]['size'] = sum(
                    len(el) 
                    for el in base_content_lines[
                        files[example['base_path']]['old_change_start']:
                        files[example['base_path']]['old_change_end']
                    ]
                )
                ttl_sz = sum(el["size"] for el in files.values())
                if ttl_sz+c_length > render_params.max_pr_length or len(files) == 0:
                    raise RenderError('base code size')

            
    for el in files.values():
        prepare_base(el, render_params)
        
    ttl_sz = sum(len(el["base_content"]) for el in files.values())
    files = {
        path : file 
        for path, file in files.items()
        if not get_if_to_remove_file_by_subsample(path, file, render_params, ttl_sz)
    }
    
    ttl_sz = sum(len(el["base_content"]) for el in files.values())
    if ttl_sz+c_length > render_params.max_pr_length or len(files) == 0:
        raise RenderError('base code size')

    lang_distr = []
    for k, v in files.items():
        lang_distr.append([v['base_language'], k, len(v["base_content"])])
    
    res = BASE_TOKEN
    for base_path, el in files.items():
        res += FILE_PATH_TOKEN + base_path
        res += CODE_TOKEN + el['base_content']
        del el['base_content']
    return res, files, lang_distr

def render_diff_hunks_for_file(example, base_info, render_params):
    assert not base_info is None

    # the file could have been removed in base rendering as a result of subsampling
    if not example['base_path'] in base_info:
        return ''

    base_code = example["base_content"].splitlines()
    head_code = example["head_content"].splitlines()

    base_code = base_code[base_info[example['base_path']]['old_change_start']:]
    head_code = head_code[base_info[example['base_path']]['old_change_start']:]

    if render_params.randomness_on:
        context = np.random.randint(
            render_params.commit_diff_hunk_min_extra_lines_range,
            render_params.commit_diff_hunk_max_extra_lines_range
        )
    else:
        context = (
            render_params.commit_diff_hunk_max_extra_lines_range -
            render_params.commit_diff_hunk_min_extra_lines_range
        ) // 2

    groups = []
    block = []
    for group in SequenceMatcher(
        None,
        base_code,
        head_code
    ).get_grouped_opcodes():
        for el in group:
            if el[0] == 'equal':
                continue
            if render_params.join_commit_diff_hunks:
                if len(block) == 0 or el[1] - context <= block[-1][2]:
                    block.append(el)
                else:
                    groups.append(block)
                    block = [el]
            else:
                groups.append([el])
    if len(block) > 0:
        groups.append(block)

    res = FILE_PATH_TOKEN + example['base_path']

    last_base = 0
    for i, el in enumerate(groups):
        start_context = context + min(el[0][1]-context-last_base, 0)
        stop_context = min(
            context,
            (groups[i+1][0][1] if i < len(groups)-1  else len(base_code)-1)-el[-1][2]
        )

        start_line_base = el[0][1] - start_context
        stop_line_base = el[-1][2] + stop_context

        start_line_head = el[0][3] - start_context
        stop_line_head = el[-1][4] + stop_context

        # in editors and git diff lines starts from 1
        res += (
            f'{DIFF_HUNK_TOKEN}@@ -{start_line_base+1},{stop_line_base-start_line_base}'+
            f' +{start_line_head+1},{stop_line_head-start_line_head} @@\n'
        )
    
        last_base = el[-1][2]
        for i2 in range(start_line_base, el[0][1]):
            res += ' '+base_code[i2]+'\n'

        for i1 in range(len(el)):
            for i2 in range(el[i1][1], el[i1][2]):
                res += '-'+base_code[i2]+'\n'
            for i2 in range(el[i1][3], el[i1][4]):
                res += '+'+head_code[i2]+'\n'
            if i1 < len(el)-1:
                for i2 in range(el[i1][2], el[i1+1][1]):
                    res += ' '+base_code[i2]+'\n'

        for i2 in range(el[-1][2], stop_line_base):
            res += ' '+base_code[i2]
            if i2 < stop_line_base - 1:
                res += '\n'

    return res

def render_commit_diff_as_hunks(data, base_info, render_params, c_length):
    res = f"{PULL_REQUEST_DIFF_TOKEN}"
    if int(data['num_diff_files']) == 0:
        return res + DIFF_OMITTED_PLACEHOLDER
    
    for example in data['head_base_files']:
        if example["base_content"] is None or example["head_content"] is None:
            continue
        res += render_diff_hunks_for_file(example, base_info, render_params)
        if c_length + len(res) > render_params.max_pr_length:
            raise RenderError()
    return res

def is_approved_count(row):
    return sum(
        1 for el in row['events']
        if el['type'] == 'PullRequestReviewEvent' and el['review.state'] == 'approved'
    )

def is_merged(row):
    return any(el['pull_request.merged'] for el in row['events'])

def get_cnt_rebases(row):
    events = row['events']
    cnt = len(set(event['base_commit_id'] for event in events if not event['base_commit_id'] is None)) - 1
    if is_merged(row):
        cnt -= 1
    return max(cnt, 0)

def split_events(events):
    diffs = []
    others = []
    for el in events:
        if el['type'] == 'PullRequestCommitEvent':
            diffs.append(el)
        else:
            others.append(el)
    return others, diffs

def has_initial_diff(events, diffs):
    diff_commits = set()
    event_commits = list()
    for el in diffs:
        diff_commits.add((el['head_commit_id'], el['base_commit_id']))
        
    for el in events:
        if el['head_commit_id'] is None or el['base_commit_id'] is None:
            continue
        event_commits.append((pd.to_datetime(el['created_at']), (el['head_commit_id'], el['base_commit_id'])))

    if len(diff_commits) == 0:
        return False
    event_commits.sort()
    assert len(event_commits) > 0
    return event_commits[0][1] in diff_commits

def render_diff_if_needed(event, diffs_dict, head_commit_id, base_info, render_params, c_length):
    # if new head commit id it means conversatin is other for the previous one and we 
    # print this new id diff hunks to reflect changes resulted from the conversation
    res = ''
    if ((event['type'] == 'PullRequestEvent' or
        event['type'] == 'PullRequestReviewCommentEvent' or
        event['type'] == 'PullRequestReviewEvent') and
        head_commit_id != event['head_commit_id'] 
    ):
        assert not event['head_commit_id'] is None
        head_commit_id = event['head_commit_id']
        if event['head_commit_id'] in diffs_dict:
            res += render_commit_diff_as_hunks(
                diffs_dict[event['head_commit_id']],
                base_info, render_params, c_length
            )
    return head_commit_id, res

def remove_blacklisted_languages(diffs, language_blacklist):
    res = []
    for diff in diffs:
        idx_to_retain = []
        for i, file in enumerate(diff['head_base_files']):
            if len(language_blacklist[
                (language_blacklist['language'] == file['base_language']) &
                (language_blacklist['extension'] == Path(file['base_path']).suffix.strip('.'))
            ]) > 0:
                continue
            idx_to_retain.append(i)
        if len(idx_to_retain) == 0:
            continue
        diff['head_base_files'] = diff['head_base_files'][idx_to_retain]
        res.append(diff)
    return res

def render_pr(row, df_commit_pairs, render_params, return_render, language_blacklist):
    if return_render:
        row['render'] = ''
    row['len'] = 0
    row['lang_distr'] = {}
    if row['repo_name'] == 'Chocogrenouille/github-slideshow':
        return row

    # at some versions events are a json string
    # or corrupted
    try:
        if type(row['events']) is str:
            row['events'] = json.loads(row['events'])
    except Exception:
        return row

    # those are filtered out at earlier stage
    if not(is_approved_count(row) > 0 or is_merged(row)):
        return row
    # remove pr with multiple bases for simplicity
    if get_cnt_rebases(row) > 0:
        return row

    if df_commit_pairs is None:
        events, diffs = split_events(row['events'])
    else:
        diffs = df_commit_pairs[
            df_commit_pairs['pull_request.guid']  == row['pull_request.guid']
        ].to_dict(orient='records')
        events, _ = split_events(row['events'])

    if not language_blacklist is None:
        diffs = remove_blacklisted_languages(diffs, language_blacklist)
    
    if len(diffs) == 0:
        return row

    if not has_initial_diff(events, diffs):
        return row

    if render_params.randomness_on and render_params.subsample_pr_per_repo:
        pr_per_repo = row['pr_count_per_repo']
        # decrease lineraly from .8 prob until .1 and then reciprocally to the number pr per repo
        # leaving max around 100 pr for big pr per repo
        if pr_per_repo <= 1000:
            r = -0.0007 * pr_per_repo + .8007
        else:
            r = 100 / pr_per_repo

        if np.random.random() >= r:
            return row

    if render_params.anonymise_usernames:
        events = ppre.replace_usernames(events)

    res = ''
    base_info = None
    lang_distr = None
    head_commit_id = ''

    # we have removed all the PRs with more than one base so index by head only
    diffs_dict = {}
    for el in diffs:
        diffs_dict[el['head_commit_id']] = el

    try:
        open_event_rendered = False
        for event in events:
            if event['actor.login'] is None:
                event['actor.login'] = ''
            if event['type'] == 'PullRequestEvent':
                # Sometimes before open evnt there are old close events, for example
                if event['action'] != 'opened':
                    if open_event_rendered:
                        # render diff before event with new head for other events
                        head_commit_id, c_rendering = render_diff_if_needed(
                            event, diffs_dict, head_commit_id, base_info, render_params, len(res)
                        )
                        res += c_rendering
                res += render_pr_event(event, open_event_rendered, render_params, len(res))
                # we need rendering of base only once as we remove all PR with multiple bases
                if (open_event_rendered == False and 
                    (event['action'] == 'opened' or event['action'] == 'reopened')
                ):
                    open_event_rendered = True
                    render_data, base_info, lang_distr = render_base_files(diffs, render_params, len(res))
                    res += render_data
                    # render diff after base files for open event
                    head_commit_id, c_rendering = render_diff_if_needed(
                        event, diffs_dict, head_commit_id, base_info, render_params, len(res)
                    )
                    res += c_rendering
            elif (
                event['type'] == 'PullRequestReviewCommentEvent' or
                event['type'] == 'PullRequestReviewEvent'
            ):
                # render diff before event with new head
                # TODO: check manually whose events which have review events before open
                if open_event_rendered:
                    head_commit_id, c_rendering = render_diff_if_needed(
                        event, diffs_dict, head_commit_id, base_info, render_params, len(res)
                    )
                    res += c_rendering
                    res += render_pr_review_event(event, base_info, render_params, len(res))
            elif (
                event['type'] == 'IssueEvent' or
                event['type'] == 'IssueCommentEvent'
            ):
                # does not have head and base info so no diff render
                res += render_issue(event, render_params, len(res))
            elif event['type'] == 'PullRequestCommitEvent':
                raise RuntimeError(f"PullRequestCommitEvent must not be in the main loop")
            else:
                raise ValueError(f"unknown event type {event['type']}")
  
            if len(res) > render_params.max_pr_length:
                raise RenderError()
    except RenderError:
        return row
    except Exception:
        raise
    if len(res) < render_params.min_pr_length:
        return row
    if len(res) > render_params.max_pr_length:
        return row
    if return_render:
        row['render'] = res
    row['len'] = len(res)
    row['lang_distr'] = lang_distr
    return row

@ray.remote
def get_renders_for_bucket(
    source, commits_source=None,
    render_params=None,
    return_render=True, return_lang_distr=False, return_data=False,
    base_seed=42, seed=None,
    dst_file_name=None,
    language_blacklist=None
):
    
    must_be_df = False
    if issubclass(type(source), pd.DataFrame):
        df = source
        must_be_df = True
        # if source is not path seed per bucket must be provided
        
    else:
        try:
            df = pd.read_parquet(source)
        except (pa.lib.ArrowNotImplementedError, pa.lib.ArrowInvalid):
            return source

    assert not seed is None
    seed += base_seed
    np.random.seed(seed)

    if not 'pr_count_per_repo' in df.columns:
        return pd.DataFrame(columns=['pull_request.guid', 'len', 'lang_distr'])

    if commits_source is None:
        df_cs = None
    else:
        if issubclass(type(commits_source), pd.DataFrame):
            df_cs = commits_source
        else:
            if must_be_df:
                raise ValueError("if source is DataFrame commits_source must be as well")
            try:
                # for some reason sometines it fails to read with pandas
                df_cs = pd.read_parquet(commits_source / source.name)
            except Exception:
                df_cs  = pl.read_parquet(commits_source / source.name).to_pandas()

    if render_params is None:
        render_params = RenderParams()
    
    df = df.apply(lambda x: render_pr(x, df_cs, render_params, return_render, language_blacklist), axis=1)
    df = df[df['len'] > 0]
    if not return_data:
        ret_columns = ['pull_request.guid', 'len']
        if return_render:
            ret_columns.append('render')
        if return_lang_distr:
            ret_columns.append('lang_distr')
        df = df[ret_columns]
    if dst_file_name is None:
        return df
    util.df_to_parquet_safe(df, dst_file_name)


def pr_pretty_print(text):
    splits = [m.start() for m in re.finditer(ALL_TOKENS_RE, text)]
    splits.append(len(text))
    for i in range(len(splits)-1):
        print(text[splits[i]:splits[i+1]])
   