import pandas as pd
import json

import issue_parser
import pr_parser
import util

columns = [
    'group_id',
    'type',
    'action',
    'actor.login',
    'actor.is_bot',
    'created_at',
    'repo.name',
    'org.login',
    'pull_request.guid',
    'public',

    'title',
    'text',

    'issue_id',
    'issue_number',
    'comment_id',
    # Those are needed to re-create open event if needed
    'issue_created_at',
    'issue_user_login',
    

    'author_association',
    'review_comment.id',
    'review_comment.commit_id',
    'review.state',

    'comment.pull_request_review_id',
    'comment.in_reply_to_id',
    'comment.path',
    'comment.original_position',
    'comment.diff_hunk',

    'pull_request.merged',
    'head_commit_id',
    'base_commit_id',
    'base_repo_name',
    'head_repo_name',
    # these columns are needed to later re-create pr open event if needed
    'pull_request.created_at',
    'pull_request.user.login',
    'pull_request.title',
    'pull_request.body',
]
    

def parse_files(filenames, i, dst):
    issue_events = set([ 'IssueCommentEvent', 'IssuesEvent',])
    events = []
    for filename in filenames:
        for line in util.enumerate_gz_jsonl(filename):
            if line is None:
                continue
            try:
                event_type = line['type']
                parsed_line = None
                if event_type in issue_events:
                    parsed_line = issue_parser.process_line(line)
                elif event_type == 'PullRequestReviewCommentEvent':
                    parsed_line = pr_parser.get_data_PullRequestReviewCommentEvent(
                        line
                    )
                elif event_type == 'PullRequestReviewEvent':
                    parsed_line = pr_parser.get_data_PullRequestReviewEvent(
                        line
                    )
                elif event_type == 'PullRequestEvent':
                    parsed_line = pr_parser.get_data_PullRequestEvent(
                        line
                    )
                if parsed_line is not None:
                    parsed_line['actor.is_bot'] = util.get_is_user_bot(parsed_line)
                    events.append(parsed_line)
            except Exception as e:
                raise ValueError(parsed_line) from e
    
    if len(events) == 0:
        return None
    
    df = pd.DataFrame(events, columns=columns)
    df['pull_request.merged'] = df['pull_request.merged'].astype(bool)
    util.df_to_parquet_safe(df, dst / f'{i}.parquet')


def separate_prs_from_issues(df):
    df_issues = df[df['pull_request_user_login'].isna()]
    df_pr_issues = df[df['pull_request_user_login'].isna() == False]
    return df_issues, df_pr_issues

def format_issues(df):
    df['created_at'] = pd.to_datetime(df['created_at'])
    df = df.sort_values('created_at')
    res = []
    for gr in df.groupby('issue_id'):
        res.append(issue_parser.parse_issue_history(gr))
    df = pd.DataFrame(res)
    return df

def get_pr_guid(pr_data):
    return f"{pr_data['user_login']}/{pr_data['repo']}/pull/{int(pr_data['number'])}"

def get_pr_guid_from_from_pr_columns(r):
    return f"{r['pull_request_user_login']}/{r['pull_request_repo']}/pull/{int(r['pull_request_number'])}"

def merge_pr_issues_pr_events(df_pr, df_issues):
    # todo: check why they even appear
    df_pr = df_pr.drop_duplicates('pull_request.guid')
    df_pr = df_pr.reset_index(drop=True)
    
    # todo: check why they even appear
    df_issues = df_issues.drop_duplicates('pull_request.guid')
    df_issues = df_issues.reset_index(drop=True)

    df_all = df_pr.merge(df_issues, on='pull_request.guid', how='outer')
    return df_all

def parse_grouped_files(file, prs_dest):
    df = util.pandas_read_parquet_ex(file)
    # Issues are written back to the source file
    # so check if they are already processeds
    if 'events' in df.columns:
        return 1

    issue_events = set(['IssueCommentEvent', 'IssuesEvent',])
    is_issue = df['type'].isin(issue_events) & (df['group_id'].str.contains('/pull/') == False)
    df_issues = df[is_issue]
    df_prs = df[is_issue == False]

    
    df_issues = format_issues(df_issues)
    
    df_prs = df_prs.groupby(
        'group_id'
    ).apply(
        pr_parser.format_pull_request
    ).reset_index(
        drop=False
    ).rename(
        columns={'group_id': 'pull_request.guid'}
    )
 
    util.df_to_parquet_safe(df_prs, prs_dest / file.name)
    util.df_to_parquet_safe(df_issues, file)
    return 0