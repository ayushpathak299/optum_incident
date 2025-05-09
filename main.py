import requests
from dotenv import load_dotenv
import psycopg2
from dateutil.parser import parse
import time
import os
load_dotenv()
from datetime import datetime, timedelta

# === Jira Config ===
JIRA_URL = os.getenv("JIRA_URL")
JIRA_USERNAME = os.getenv("JIRA_USERNAME")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")

# === Database Config ===
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# === Connect DB ===
conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
cursor = conn.cursor()

def insert_issue_data(issue_data):
    cleaned_data = {}
    for k, v in issue_data.items():
        if isinstance(v, dict):
            cleaned_data[k] = str(v)
        elif isinstance(v, list):
            cleaned_data[k] = ', '.join(map(str, v))
        else:
            cleaned_data[k] = v

    columns = ', '.join(f'"{k}"' for k in cleaned_data.keys())
    placeholders = ', '.join(['%s'] * len(cleaned_data))
    updates = ', '.join(f'"{k}" = EXCLUDED."{k}"' for k in cleaned_data.keys() if k != 'issue_id')
    values = list(cleaned_data.values())

    cursor.execute(f"""
        INSERT INTO optum_incident ({columns})
        VALUES ({placeholders})
        ON CONFLICT (issue_id) DO UPDATE SET
        {updates};
    """, values)
    conn.commit()

def fetch_issues(jql_query, fields):
    url = f"{JIRA_URL}/rest/api/2/search"
    start_at = 0
    all_issues = []

    while True:
        params = {
            "jql": jql_query,
            "maxResults": 100,
            "startAt": start_at,
            "fields": fields,
            "expand": "changelog"
        }
        response = requests.get(url, auth=(JIRA_USERNAME, JIRA_API_TOKEN), params=params)

        if response.status_code != 200:
            print("❌ Jira fetch error:", response.text)
            break

        issues = response.json().get("issues", [])
        if not issues:
            break

        all_issues.extend(issues)
        start_at += len(issues)

    return all_issues

def process_issue(issue):
    issue_id = issue['key']
    fields = issue['fields']
    changelog = issue.get('changelog', {}).get('histories', [])

    client_field_obj = fields.get('customfield_12310')
    client_name = client_field_obj.get('value') if client_field_obj else None
    team_obj = fields.get('customfield_10900')
    team = team_obj.get('title') if team_obj else None

    assignee = fields.get('assignee', {}).get('displayName') if fields.get('assignee') else None
    components = ', '.join([c['name'] for c in fields.get('components', [])]) if fields.get('components') else None
    status = fields.get('status', {}).get('name')
    priority = fields.get('priority', {}).get('name')
    summary = fields.get('summary')
    reporter = fields.get('reporter', {}).get('displayName')
    created = fields.get('created')

    with_core_time = done_time = pending_close_time = None
    for history in changelog:
        for item in history['items']:
            if item['field'] == 'status':
                if item['toString'] == 'With Core Product':
                    with_core_time = history['created']
                elif item['toString'] == 'Done':
                    done_time = history['created']
                elif item['toString'] == 'Pending Close':
                    pending_close_time = history['created']

    linked_keys = []
    for link in fields.get('issuelinks', []):
        linked_issue = link.get('outwardIssue') or link.get('inwardIssue')
        if linked_issue:
            linked_keys.append(linked_issue['key'])

    created_dt = parse(created)
    time_to_core = (parse(with_core_time) - created_dt).days if with_core_time else None
    time_to_done = (parse(done_time) - created_dt).days if done_time else None
    time_to_pending = (parse(pending_close_time) - created_dt).days if pending_close_time else None

    issue_data = {
        'issue_id': issue_id,
        'client_name': client_name,
        'team': team,
        'assignee': assignee,
        'components': components,
        'status': status,
        'priority': priority,
        'summary': summary,
        'reporter': reporter,
        'created': created,
        'with_core_product': with_core_time,
        'done': done_time,
        'pending_close': pending_close_time,
        'time_to_core_product': time_to_core,
        'time_to_done': time_to_done,
        'time_to_pending_close': time_to_pending,
        'linked_issue_keys': ', '.join(linked_keys) if linked_keys else None
    }

    insert_issue_data(issue_data)

def main():
    from datetime import datetime, timedelta, timezone

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    jql = f'project = OI AND (created >= "{yesterday}" OR updated >= "{yesterday}")'
    fields = "customfield_12310,customfield_10900,assignee,components,status,priority,summary,reporter,created,issuelinks"
    issues = fetch_issues(jql, fields)

    for issue in issues:
        process_issue(issue)
        time.sleep(0.3)

    cursor.close()
    conn.close()
    print("✅ All issues processed.")

if __name__ == "__main__":
    main()
