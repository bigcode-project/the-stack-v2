import pandas as pd
from pathlib import Path

from typing import List, Optional
from pydantic import BaseModel, Extra

import util

def get_pr_guid(pr_data):
    split = pr_data['url'].split('/')
    return f'{split[-4]}/{split[-3]}/pull/{split[-1]}'

def get_value(data, path):
    keys = path.split('.')
    for key in keys:
        if not isinstance(data, dict):
            data = {}
        data = data.get(key, {})
    return None if (isinstance(data, dict) and not data) else data

def flattern(data, prefix, keys):
    res = {}
    for key in keys:
        res[prefix+'.'+key] = get_value(data, key)
    return res

def get_general_pull_request_data(data):
    return {
        'pull_request.guid': get_pr_guid(get_value(data, 'payload.pull_request')),
        'pull_request.merged': get_value(data, 'payload.pull_request.merged'),
        'head_commit_id': get_value(data, 'payload.pull_request.head.sha'),
        'base_commit_id': get_value(data, 'payload.pull_request.base.sha'),
        'base_repo_name': get_value(data, 'payload.pull_request.base.repo.full_name'),
        'head_repo_name': get_value(data, 'payload.pull_request.head.repo.full_name'),
        # these columns are needed to later re-create pr open event if needed
        'pull_request.created_at': get_value(data, 'payload.pull_request.created_at'),
        'pull_request.user.login': get_value(data, 'payload.pull_request.user.login'),
        'pull_request.title': get_value(data, 'payload.pull_request.title'),
        'pull_request.body': get_value(data, 'payload.pull_request.body'),
    }
   
def get_data_PullRequestReviewEvent(data):
    assert get_value(data, 'type') == 'PullRequestReviewEvent'
    res =  {
        'type': get_value(data, 'type'),
        'action': get_value(data, 'payload.action'),
        'actor.login': get_value(data, 'actor.login'),
        'user.type': get_value(data, 'payload.review.user.type'),
        'repo.name': get_value(data, 'repo.name'),
        'public': get_value(data, 'public'),
        'created_at': get_value(data, 'created_at'),
        'org.login': get_value(data, 'org.login'),
        
        'text': get_value(data, 'payload.review.body'),
        'author_association': get_value(data, 'payload.review.author_association'),
        'review_comment.id': get_value(data, 'payload.review.id'),
        'review_comment.commit_id':  get_value(data, 'payload.review.commit_id'),
        'review.state': get_value(data, 'payload.review.state'),
    }
    res.update(get_general_pull_request_data(data))
    res['group_id'] = res['pull_request.guid']
    return res

def get_data_PullRequestReviewCommentEvent(data):
    assert get_value(data, 'type') == 'PullRequestReviewCommentEvent'
    res =  {
        'type': get_value(data, 'type'),
        'action': get_value(data, 'payload.action'),
        'actor.login': get_value(data, 'actor.login'),
        'user.type': get_value(data, 'payload.comment.user.type'),
        'repo.name': get_value(data, 'repo.name'),
        'public': get_value(data, 'public'),
        'created_at': get_value(data, 'created_at'),
        'org.login': get_value(data, 'org.login'),

        'author_association': get_value(data, 'payload.comment.author_association'),
        'review_comment.id': get_value(data, 'payload.comment.id'),
        'text': get_value(data, 'payload.comment.body'),
        'review_comment.commit_id': get_value(data, 'payload.comment.original_commit_id'),
    }

    res.update(flattern(get_value(data, 'payload.comment'), 'comment', [
        'pull_request_review_id',
        'in_reply_to_id',
        'path',
        'original_position',
        'diff_hunk',
    ]))

    res.update(get_general_pull_request_data(data))
    res['group_id'] = res['pull_request.guid']
    return res

def get_data_PullRequestEvent(data):
    assert get_value(data, 'type') == 'PullRequestEvent'
    res =  {
        'type': get_value(data, 'type'),
        'action': get_value(data, 'payload.action'),
        'actor.login': get_value(data, 'actor.login'),
        'user.type': get_value(data, 'payload.review.user.type'),
        'repo.name': get_value(data, 'repo.name'),
        'public': get_value(data, 'public'),
        'created_at': get_value(data, 'created_at'),
        'org.login': get_value(data, 'org.login'),

        'title': get_value(data, 'pull_request.title'),
        'text': get_value(data, 'payload.pull_request.body'),
    }
    
    res.update(get_general_pull_request_data(data))
    res['group_id'] = res['pull_request.guid']
    return res


def normalize_event(el):
    el_out = {}
    try:
        
        el_out['pull_request.guid'] = el['pull_request.guid'] if 'pull_request.guid' in el else None
        
        el_out['type'] = el['type']
        el_out['action'] = el['action']

        # these columns are needed to later re-create pr open event if needed
        el_out['pull_request.created_at'] = el['pull_request.created_at'] if 'pull_request.created_at' in el else None
        el_out['pull_request.user.login'] = el['pull_request.user.login'] if 'pull_request.user.login' in el else None
        el_out['pull_request.title'] = el['pull_request.title'] if 'pull_request.title' in el else None
        el_out['pull_request.body'] = el['pull_request.body'] if 'pull_request.body' in el else None

        # these columns are needed to re-create issue open event if needed
      
        if el['type'] == 'IssuesEvent' or el['type'] == 'IssueCommentEvent':
            el_out['created_at'] = pd.to_datetime(el['event_created_at']).to_pydatetime()
            el_out['actor.login'] = el['event_actor_name']
        else:
            el_out['created_at'] = pd.to_datetime(el['created_at']).to_pydatetime()
            el_out['actor.login'] = el['actor.login']

        el_out['actor.is_bot'] = util.get_is_user_bot(el)
    
        if el['type'] == 'PullRequeLstReviewEvent':
            el_out['author_association'] = el['review.author_association']
        elif el['type'] == 'PullRequestReviewCommentEvent': 
            el_out['author_association'] = el['comment.author_association']
        else:
            el_out['author_association'] = None
            
        if el['type'] == 'IssuesEvent':
            el_out['title'] = el['issue_title']
        else:
            el_out['title'] = el['pull_request.title'] if 'pull_request.title' in el else None
    
        if el['type'] == 'IssueCommentEvent':
            el_out['text'] = el['comment_body']
        elif el['type'] == 'IssuesEvent':
            el_out['text'] = el['issue_body']
        elif el['type'] == 'PullRequestEvent':
            el_out['text'] = el['pull_request.body']
        elif el['type'] == 'PullRequestReviewEvent':
            el_out['text'] = el['review.body']
        elif el['type'] == 'PullRequestReviewCommentEvent':
            el_out['text'] = el['comment.body']
    
        el_out['comment_id'] = el['comment_id'] if 'comment_id' in el else None
        
    
        
        el_out['labels'] = el['labels'] if 'labels' in el else None
        el_out['assignee'] = el['assignee'] if 'assignee' in el else None
        
        el_out['pull_request.merged'] = el['pull_request.merged'] if 'pull_request.merged' in el else None
    
        if el['type'] == 'PullRequestReviewEvent':
            el_out['review_comment.id'] = el['review.id']
        elif el['type'] == 'PullRequestReviewCommentEvent': 
            el_out['review_comment.id'] = el['comment.id']
        else:
            el_out['review_comment.id'] = None
        
        if el['type'] == 'PullRequestReviewEvent':
            el_out['review_comment.commit_id'] = el['review.commit_id']
        elif el['type'] == 'PullRequestReviewCommentEvent': 
            el_out['review_comment.commit_id'] = el['comment.original_commit_id']
        else:
            el_out['review_comment.commit_id'] = None
        
        if  el['type'] == 'PullRequestCommitEvent':
            el_out['head_commit_id'] = el['head_commit_id']
            el_out['base_commit_id'] = el['base_commit_id']
        else:
            el_out['head_commit_id'] = el['pull_request.head.sha'] if 'pull_request.head.sha' in el else None
            el_out['base_commit_id'] = el['pull_request.base.sha'] if 'pull_request.base.sha' in el else None
    
        if 'repo.name' in el:
            el_out['base_repo_name'] = el['repo.name'] if 'repo.name' in el else None
        else:
            el_out['base_repo_name'] = el['pull_request.base.repo.full_name'] if 'pull_request.base.repo.full_name' in el else None
        el_out['head_repo_name'] = el['pull_request.head.repo.full_name'] if 'pull_request.head.repo.full_name' in el else None
        
        el_out['review.state'] = el['review.state'] if 'review.state' in el else None
    
        el_out['comment.pull_request_review_id'] = el['comment.pull_request_review_id'] if 'comment.pull_request_review_id' in el else None
        el_out['comment.in_reply_to_id'] = el['comment.in_reply_to_id'] if 'comment.in_reply_to_id' in el else None
        el_out['comment.path'] = el['comment.path'] if 'comment.path' in el else None
        el_out['comment.original_position'] = el['comment.original_position'] if 'comment.original_position' in el else None
        el_out['comment.diff_hunk'] = el['comment.diff_hunk'] if 'comment.diff_hunk' in el else None
        
        el_out['num_diff_files'] = el['num_diff_files'] if 'num_diff_files' in el else None
        el_out['head_base_files'] = el['head_base_files'] if 'head_base_files' in el else None
    except Exception:
        print(el)
        raise
    return el_out

cols_to_del = [
    'group_id',
    'issue_id',
    'issue_number',
    'comment_id',
    'issue_created_at',
    'issue_user_login',
    'pull_request.created_at',
    'pull_request.user.login',
    'pull_request.title',
    'pull_request.body',
]
def clean_columns(el):
    for key in cols_to_del:
        del el[key]
    return el

def add_pa_open_event(events):
    pr_row = None
    has_pr_opened = False
    for event in events:
        if (
            event['type'] == 'PullRequestEvent' and
            event['action'] == 'opened' 
        ):
            has_pr_opened = True
        if pr_row is None:
            pr_row = event.copy()
    if has_pr_opened == False:
        if pr_row is None:
            print(list(events))
            raise ValueError()
        try:
            pr_row['type'] = 'PullRequestEvent'
            pr_row['action'] = 'opened'
            pr_row['created_at'] = pr_row['pull_request.created_at']
            pr_row['actor.login'] = pr_row['pull_request.user.login']
            pr_row['title'] = pr_row['pull_request.title']
            pr_row['text'] = pr_row['pull_request.body']
        except Exception:
            print(pr_row)
            raise
        events  = [pr_row] + events
    return events
    

def remove_issue_open_event(events):
    for i, event in enumerate(events):
        if (
            event['type'] == 'IssuesEvent' and 
            event['action'] == 'opened'
        ):
            del events[i]
            break
    return events

def format_pull_request(gr):
    key =  gr.iloc[0, :]['group_id']
    pr_data = gr.to_dict(orient='records')
    
    issue_event_types = set(['IssuesEvent', 'IssueCommentEvent'])
    issue_events = []
    pr_events = []
    for el in pr_data:
        if el['type'] in issue_event_types:
            issue_events.append(el)
        else:
            pr_events.append(el)

    if len(pr_events) == 0:
        return None
    
    pr_events = add_pa_open_event(pr_events)
    issue_events = remove_issue_open_event(issue_events)

    events  = pr_events + issue_events

    row = {}
    row['events'] = sorted([clean_columns(el) for el in events], key=lambda x: x['created_at'])
    return pd.Series(row)