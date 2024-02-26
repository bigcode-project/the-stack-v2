This folder contains steps necessary to reproduce dataset of Issued and Pull Requests.

## Ray scluster

Most of the steps are designed to be executed on a Ray cluster. If the code is not run on the AI Toolkit, one must implement its own cluster provisioning and management. Specifically, the scaling up and down of the Ray cluster should be implemented in `ray_server.py`, or the cluster needs to be scaled up elsewhere, and the `scale_cluster` function may not have any effect. Additionally, all paths are intended to be accessible from all cluster nodes.

Most of the processing was done on 60 nodes cluster with 4 cores and 128Gb of RAM each.

## Configuration

All configuration is in the `cfg.py`. Configs needed to change would be:
- `root_path` - a path for all processing steps and results to be wrtiten to

## 0_get_gharchive_events.py
Downloads evnets from the GHArchive. Done on one thread and with a delay in order to not overvelm the server.

## 2_parse_pull_request_events.ipynb
Extracts Issues and PRs information from the events, groups events by Issue or PR id, combines them into Issues or PR and splits to Issue and PRs.
- `issues` dataset is stored by default in `root_path/issues_prs_grouped`
- `pull requests` are stored by default in `root_path/pr_grouped` for further processing

# 2,3 ... (TBD)

