import gzip
from pathlib import Path
import pandas as pd
import numpy as np
import pickle
from tqdm.auto import tqdm
import polars as pl

import util

def process_event(line):
    event_data = {
        'type': line['type'],
        'actor.login' : line['actor']['login'],
        'public': line['public'],
        'created_at': line['created_at'],
        'repo.name': line['repo']['name'],
        'org.login': line['org']['login'] if 'org' in line else None 
    }
    return event_data

def process_issue(line):
    if not 'payload' in line or line['payload'] is None or len(line['payload']) == 0:
        return None
    action_issue_data = {
        'action': line['payload']['action'],
        'title': line['payload']['issue']['title'],
        'text': line['payload']['issue']['body'], 
        'issue_id': line['payload']['issue']['id'],
        'issue_number': line['payload']['issue']['number'],
        # Those are needed to re-create open event if needed
        'issue_created_at': line['payload']['issue']['created_at'],
        'issue_user_login' : line['payload']['issue']['user']['login'],
    }
    return action_issue_data

def process_line(line):
    event_data = process_event(line)

    action_issue_data = process_issue(line)
    if action_issue_data is None:
        return None
    event_data.update(action_issue_data)

    if 'comment' in line['payload']:
        event_data['comment_id'] = line['payload']['comment']['id']
        event_data['text'] = line['payload']['comment']['body']
    
    # if it is part of pull request grouping will be by its id, otherwise by issue id
    if 'pull_request' in line['payload']['issue'] and 'url' in line['payload']['issue']['pull_request']:
        split = line['payload']['issue']['pull_request']['url'].split('/')
        #'pull_request_user_login': split[-4],
        #'pull_request_repo': split[-3],
        #'pull_request_number': split[-1]
        event_data['pull_request.guid'] = f"{split[-4]}/{split[-3]}/pull/{split[-1]}"

        event_data['group_id'] = event_data['pull_request.guid']
    else:
        event_data['pull_request.guid'] = None
        event_data['group_id'] = str(event_data['issue_id'])

    return event_data


def parse_comment(r):
    # actions: created, edited or deleted
    comment = {
        'type': 'comment',
        'action': r['action'],
        'datetime': str(r['created_at']),
        'author': r['actor.login'],
        'comment_id': int(r['comment_id']),
    }
    if r['action'] != 'deleted':
        comment['comment'] = r['text']
    return comment

def parse_issue(r, force_open=False):

    if force_open:
        # if issue was from before 2015 or because of gharchive outage
        # it will not have open event, so we emulate it
        issue =  {
            'type': 'issue',
            'action': 'opened',
            'datetime': str(r['issue_created_at']),
            'author': r['issue_user_login'],
            'title': r['title'],
            'description': r['text']
        }
    else:
        issue = {
            'type': 'issue',
            'action': r['action'],
            'datetime': str(r['created_at']),
            'author': r['actor.login'],
        }
        if r['action'] == 'opened' or r['action'] == 'edited' or r['action'] == 'reopened':
            # all those events can result in different title or description
            issue.update({
                'title': r['title'],
                'description': r['text']
            })

        if r['action'] == 'closed':
            # no additional info
            pass

    return issue

def parse_issue_history(gr):
    gr = gr[1]
    res_dict = {}
    indx = 0
    r = gr.iloc[0]
    res_dict['repo'] = r['repo.name']
    res_dict['org'] = r['org.login']
    res_dict['issue_id'] = int(r['issue_id'])
    res_dict['issue_number'] = int(r['issue_number'])

    # From docs: all PR are issues but not all issues are PR, for PR review comments use PR api (which is not done yet)
    # supposedly if we have PR not None it is a general discussion of some PR, so we just mark issue's PR for now
    if not pd.isna(r['pull_request.guid']):
            res_dict['pull_request.guid'] = r['pull_request.guid']
    else:
        res_dict['pull_request.guid'] = None

    res_dict['events'] = []

    for i, r in gr.iterrows():
        if indx == 0:
            if r['type'] != 'IssuesEvent' or r['action'] != 'opened':
                res_dict['events'].append(parse_issue(r, force_open=True))

        # TODO: edited events will need to be later aggregated with their open or reopen events
        #       however, for some reason no single edited event in the set, check original parser
        if r['type'] == 'IssueCommentEvent':
            res_dict['events'].append(parse_comment(r))
        elif r['type'] == 'IssuesEvent':
            res_dict['events'].append(parse_issue(r))
        else:
            raise RuntimeError(f"unexpeceted event type: {r['type']}")
        indx += 1

    return res_dict

def print_issue_history(data, truncate=False):
    res = ''
    res += f"REPO: {data['repo']}\n"
    if data['org'] is not None:
        res += f"ORG: {data['org']}\n"
    res += f"ISSUE NUNBER: {int(data['issue_number'])}\n"
    
    if data['pull_request'] is not None:
        res += f"PULL REQUEST [USER: {data['pull_request']['user_login']} REPO: {data['pull_request']['repo']} NUMBER: {data['pull_request']['number']}]\n"
    res += '\n\n'
    
    for event in data['events']:
        if event['type'] == 'comment':
            res += f"COMMENT {int(event['comment_id'])} {event['action'].upper()} [{event['datetime']}] BY {event['author']}\n"
            if event['action'] != 'deleted':
                if truncate:
                    res += event['comment'][:100] + '...\n'
                else:
                    res += event['comment'] + '\n'
            res +=  '\n'
        elif event['type'] == 'issue':
            res += f"ISSUE {event['action'].upper()} [{event['datetime']}] BY {event['author']}\n"
            if event['action'] == 'edited' or event['action'] == 'opened' or event['action'] == 'reopened':
                res += f"TITLE: {event['title']}\n"
                if truncate:
                    res += f"DESCRIPTION: {event['description'][:100]}...\n"
                else:
                    res += f"DESCRIPTION: {event['description']}\n"
            else:
                pass
            res += '\n'
        else:
            pass

    return res
