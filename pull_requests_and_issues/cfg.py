from datetime import date
from pathlib import Path

root_path = Path('/data/the_stack_v2_pr_and_other_datasets')

gharchives_path = root_path / "gharchives"
sdate = date(2015, 1, 1)  # start date
edate = date(2023, 7, 26)  # end date

parsed_issues_prs_path = root_path / 'issues_prs'
issues_prs_grouped_path  = root_path /  'issues_prs_grouped'
issues_grouped_path  = root_path /  'issued_grouped'
prs_grouped_path  = root_path /  'prs_grouped'
