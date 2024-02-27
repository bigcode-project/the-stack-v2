import json
import pandas as pd
import ray
import util
from collections import defaultdict, OrderedDict
from pathlib import Path
from functools import partial
import sqlite3
import re

def get_events_as_list(row, column):
    events = [] if row[column] is None else json.loads(
        row[column]
    )
    return events

def get_events_as_list_from_issues(row, column):
    events = [] if row[column] is None else json.loads(
        row[column]
    )['events']
    return events


def add_license_to_pr_remove_non_permissive(
    dfis,
    repo_licenses_db_path='/data/the_stack_v2/repo_licenses/database.db',
    step = 10000
):
    repos = list(dfis['repo_name'].unique())
    con = sqlite3.connect(f'file:{repo_licenses_db_path}?mode=ro', uri=True)
    res = []
    for i in range(0, len(repos), step):
        res_c = con.execute(
            (
                f"SELECT repo_name, license_type FROM repo_licenses WHERE repo_name in" +
                f" ({','.join(['?']*len(repos[i:i+step]))})"
            ),
            repos[i:i+step]
        )
        res.append(res_c.fetchall())
    res = sum(res, start=[])
    dfrl = pd.DataFrame(res, columns=['repo_name', 'license_type'])
    
    dfis = dfis.merge(dfrl, on='repo_name', how='right')
    dfis = dfis[dfis['license_type'] != 'non_permissive']
    return dfis

def filter_pr_opt_out(events, users_for_issues_opt_out):
    indexes_to_keep = []
    for i, event in enumerate(events):
        # if opened by opt out user remove PR
        if (
            event['type'] == 'issue' and 
            event['action'] == 'opened' and 
            event['actor.login'] in users_for_issues_opt_out
        ):
            return None
        # if opened by opt out user or from or to opt out user repo
        # remove PR
        head_repo_name  = event['head_repo_name'] if not event['head_repo_name'] is None else ''
        base_repo_name  = event['base_repo_name'] if not event['base_repo_name'] is None else ''
        if (
            event['type'] == 'PullRequestEvent' and
            event['action'] == 'opened' and
            (
                (event['actor.login'] in users_for_issues_opt_out)  or
                (head_repo_name.split('/')[0] in users_for_issues_opt_out) or
                (base_repo_name.split('/')[0] in users_for_issues_opt_out)
            )
        ):
            return None

        # if other type of event by opt out user skip
        if event['actor.login'] in users_for_issues_opt_out:
            continue
            
        indexes_to_keep.append(i)
    
    events = [events[i] for i in indexes_to_keep]
    if len(events) == 0:
        return None
    return events


def remove_opt_out(df, repos_opt_out, users_for_repo_opt_out, users_for_issues_opt_out):
    df = df[df['repo_name'].isin(repos_opt_out) == False]
    df['username'] = df['repo_name'].str.split('/', expand=True)[0]
    df = df[df['username'].isin(users_for_repo_opt_out) == False]
    df['events'] = df['events'].apply(partial(
        filter_pr_opt_out,
        users_for_issues_opt_out=users_for_issues_opt_out
    ))
    df = df[df['events'].isna() == False]
    df = df.drop(columns='username')
    return df

def pr_stats_row(row):
    pr_events = [] 
    pr_code_review_events = [] 
    pr_issue_events = []
    for event in row['events']:
        if event['type'] == 'PullRequestEvent':
            pr_events.append(event)
        elif event['type'] == 'IssuesEvent' or event['type'] == 'IssueCommentEvent':
            pr_issue_events.append(event)
        else:
            pr_code_review_events.append(event)

    row['is_bot_opened'] = 1 if any(
        el['actor.is_bot'] and el['action'] == 'opened'
        for el in pr_events
    ) else 0
    
    row['is_closed'] = 1 if any(el['action'] == 'closed' for el in pr_events + pr_code_review_events) else 0
    row['is_merged'] = 1 if any(el['pull_request.merged'] for el in  pr_events + pr_code_review_events) else 0
    
    row['n_non_bot_users'] = len(set(
        el['actor.login']
        for el in pr_events+pr_code_review_events+pr_issue_events
        if not el['actor.is_bot']
    ))

    row['n_bot_users'] = len(set(
        el['actor.login'] 
        for el in pr_events+pr_code_review_events+pr_issue_events
        if el['actor.is_bot']
    ))

    row['n_pre'] = len(pr_events)
    row['n_pre_bot'] = sum(el['actor.is_bot'] for el in pr_events)
    row['n_cre'] = len(pr_code_review_events)
    row['n_cre_bot'] = sum(el['actor.is_bot'] for el in pr_code_review_events)
    row['n_ie'] = len(pr_issue_events)
    row['n_ie_bot'] = sum(el['actor.is_bot'] for el in pr_issue_events)

    row['n_commit_pairs'] = len(set(
        (el['base_commit_id'], el['head_commit_id'])
        for el  in pr_events + pr_code_review_events
    ))

    # number of differnet base commit different from the first
    row['n_rebases'] = len(set(
        el['base_commit_id']
        for el  in pr_events + pr_code_review_events
    )) - 1
    if row['is_merged']:
        # if is merged then sometimes base has one more commit id at the end
        row['n_rebases'] -= 1
    if row['n_rebases'] < 0:
        row['n_rebases'] = 0

    # NOTE: this was used for collecting commit pairs to get fiels form SWH
    #res['commit_pairs'] = set(
    #    (el['pull_request.head.sha'], el['pull_request.base.sha'])
    #    for el  in pr_events + pr_code_review_events
    #)
    #res['n_commit_pairs'] = len(res['commit_pairs'])
    
    if len(pr_events) > 0:
        row['pr_desc_len'] = len(pr_events[0]['text']) if pr_events[0]['text'] is not None else 0
        row['pr_title_len'] = len(pr_events[0]['title']) if pr_events[0]['title'] is not None else 0
    else:
        row['pr_desc_len'] = 0
        row['pr_title_len'] = 0
        
    
    # events are sorted by time so later will override ealier ones
    rv = {}
    for el in pr_code_review_events:
        if el['type'] == 'PullRequestReviewEvent':
            rv[el['review_comment.id']] = el['review.state']
    st = defaultdict(int)
    for v in rv.values():
        st[v] += 1
    for state in ['approved', 'dismissed', 'commented', 'changes_requested']:
        key = 'review_n_' + state
        if state in st:
            row[key] = st[state]
            del st[state]
        else:
            row[key] = 0
    row['review_n_other'] = sum(st.values())
    return row

def get_pr_stats(df):
    return df.apply(pr_stats_row, axis=1)


@ray.remote
def process_pr_bucket(
    file, dst, 
    repos_opt_out, users_for_repo_opt_out, users_for_issues_opt_out,
    min_desc_length,
    min_title_length
):
    file = Path(file)
    dst = Path(dst)
    dst_file = dst / file.name
    if dst_file.is_file():
        return 1

    stats = {}
    df = pd.read_parquet(file)
    # TODO: check why some events are empty
    df = df.dropna(subset='events')
    stats['initial'] = len(df)
    df = get_pr_stats(df)
    stats['got_stats'] = len(df)
    # filter out prs with empty description, started by bots 
    # and if no human user in comments or code review events
    df = df[
        (df['is_bot_opened'] == 0) & 
        (df['pr_desc_len'] > 0) &
        #(
        #    ((df['n_cre']) > 0) | 
        #    ((df['n_ie']) > 0)
        #)
        #(df['pr_desc_len'] >= min_desc_length) &
        #(df['pr_title_len'] >= min_title_length) & 
        (
            ((df['n_cre']-df['n_cre_bot']) > 0) | 
            ((df['n_ie']-df['n_ie_bot']) > 0)
        )
        #((df['review_n_approved'] > 0) | df['is_merged']) &
        #(df['n_rebases'] == 0)
    ]
    stats['removed_filter_on_stats'] = len(df)
    df['repo_name'] = df['pull_request.guid'].str.split('/pull/').str[0]
    df = remove_opt_out(
        df,
        repos_opt_out,
        users_for_repo_opt_out,
        users_for_issues_opt_out
    )
    stats['removevd_opt_out'] = len(df)
    df = add_license_to_pr_remove_non_permissive(df)
    stats['removed_non_pemissive'] = len(df)
    util.df_to_parquet_safe(df, dst_file)
    return stats


# from https://github.com/bigcode-project/bigcode-dataset/blob/main/preprocessing/utils/utils_issues.py
# regexes used for removing automated text
GITHUB_EMAILS = [
    re.compile(pattern, re.DOTALL)
    for pattern in [
        "(.*)From:.+Reply to this email directly.+view it on GitHub(.*)\n?(.*)",
        "(.*)On.+notifications@github.com.+wrote:.+Reply to this email directly.+view it on GitHub(.*)\n?(.*)",
        "(.*)Signed-off-by: .+<.+>(.*?)\n?(.*)",
    ]
]
GITHUB_EMAIL_DATE = re.compile("\d+/\d+/\d+ \d{2}:\d{2} [AP]M.+wrote")
GITHUB_EMAIL_LINEBREAK = re.compile("_{20,}")

def _strip_automated_email_text(text):
    """Removes text auto-generated when users post in issues via email reply"""
    if text:
        text = text.strip()
    else:
        return ""
    # try to extract with regex directly
    for pattern in GITHUB_EMAILS:
        m = pattern.match(text)
        if m:
            break
    if m:
        text = m.group(1) + m.group(3)
    else:
        # if no exact matches, apply matching line by line and
        # get potential content before/after automated email text
        lines = text.split("\n")
        start, end = 0, -1
        for i, line in enumerate(lines):
            line = line.strip()
            if "notifications@github.com" in line or bool(
                GITHUB_EMAIL_DATE.search(line)
            ):
                start = i
            if "Reply to this email directly" in line:
                end = i + 1 if line.endswith(":") else i
            if line.startswith(">"):
                # remove quoted text in replies
                end = i
        text = "\n".join(lines[:start] + lines[end + 1 :])
    # remove page break line
    return GITHUB_EMAIL_LINEBREAK.sub("", text).strip()


def replace_usernames(events):
    """Replaces real usernames (from authors list) with placeholders in GitHub issues"""
    usernames = [event['actor.login'] for event in events if not event['actor.login'] is None]
    usernames = {
        u: f"username_{i}" for i, u in enumerate(OrderedDict.fromkeys(usernames))
    }
    for event in events:
        if event['actor.login'] is None:
            continue
        event["actor.login"] = usernames[event["actor.login"]]
        for u_old, u_new in usernames.items():
            if event["text"] and u_old in event["text"]:
                    event["text"] = event["text"].replace(u_old, u_new)
    return events