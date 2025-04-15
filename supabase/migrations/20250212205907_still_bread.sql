import os
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import requests
import time
import sys

# Load environment variables
load_dotenv()

# GitHub API Token
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("GitHub token is missing.")

# Supabase configuration
SUPABASE_URL = os.getenv("VITE_SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials are missing.")

print(f"Using Supabase URL: {SUPABASE_URL}")
print("Supabase key is configured")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# GitHub API Headers
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def format_date(date_str: str) -> str:
    """Safely format a date string to ISO format."""
    try:
        if not date_str:
            return datetime.now(timezone.utc).isoformat()
        clean_date = date_str.replace('Z', '+00:00')
        return datetime.fromisoformat(clean_date).isoformat()
    except (ValueError, TypeError):
        return datetime.now(timezone.utc).isoformat()

def load_repositories():
    config_file = "config.properties"
    if not os.path.exists(config_file):
        raise FileNotFoundError("config.properties not found!")
    with open(config_file, "r") as file:
        return [line.strip().split("=")[0] for line in file]

def get_latest_date(table_name: str, repo_id: str, date_field: str) -> datetime:
    """Get the latest date from a specific table for a repository."""
    result = supabase.table(table_name)\
        .select(date_field)\
        .eq("repository_id", repo_id)\
        .order(date_field, desc=True)\
        .limit(1)\
        .execute()
    if result.data and result.data[0][date_field]:
        return datetime.fromisoformat(result.data[0][date_field])
    return datetime.now(timezone.utc) - timedelta(days=30)

def fetch_paginated_data(url, params=None):
    items = []
    page = 1
    while True:
        if params is None:
            params = {}
        params["page"] = page
        params["per_page"] = 100
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 403:
            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
            wait_time = max(reset_time - time.time(), 0)
            if wait_time > 0:
                print(f"Rate limit reached. Waiting {wait_time:.0f} seconds...")
                time.sleep(wait_time + 1)
                continue
        if response.status_code != 200:
            print(f"Error fetching {url}: {response.status_code}")
            break
        new_items = response.json()
        if not new_items:
            break
        items.extend(new_items)
        if len(new_items) < 100:
            break
        page += 1
    return items

def get_or_create_repository(repo_name):
    result = supabase.table("repositories").select("*").eq("name", repo_name).execute()
    if result.data and len(result.data) > 0:
        return result.data[0]["id"]
    result = supabase.table("repositories").insert({"name": repo_name}).execute()
    return result.data[0]["id"]

def delete_old_data(repo_id: str):
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    supabase.table("commits").delete().eq("repository_id", repo_id).lt("committed_at", thirty_days_ago.isoformat()).execute()
    print(f"Deleted old commits for repo_id: {repo_id}")
    supabase.table("pull_requests").delete().eq("repository_id", repo_id).lt("created_at", thirty_days_ago.isoformat()).execute()
    print(f"Deleted old PRs for repo_id: {repo_id}")
    supabase.table("issues").delete().eq("repository_id", repo_id).lt("created_at", thirty_days_ago.isoformat()).execute()
    print(f"Deleted old issues for repo_id: {repo_id}")
    supabase.table("reviews").delete().eq("repository_id", repo_id).lt("created_at", thirty_days_ago.isoformat()).execute()
    print(f"Deleted old reviews for repo_id: {repo_id}")

def store_repository_data(repo_name):
    print(f"Processing {repo_name}...")
    base_url = f"https://api.github.com/repos/{repo_name}"
    try:
        repo_id = get_or_create_repository(repo_name)
        print(f"Repository ID: {repo_id}")

        # Clean up old data
        delete_old_data(repo_id)

        # Get latest dates or default to 30 days ago
        latest_commit_date = get_latest_date("commits", repo_id, "committed_at")
        latest_pr_date = get_latest_date("pull_requests", repo_id, "created_at")
        latest_issue_date = get_latest_date("issues", repo_id, "created_at")
        latest_review_date = get_latest_date("reviews", repo_id, "created_at")

        # Fetch commits from all branches
        print("Fetching commits...")
        branches_url = f"{base_url}/branches"
        branches = fetch_paginated_data(branches_url)
        for branch in branches:
            branch_name = branch["name"]
            commits_url = f"{base_url}/commits"
            params = {"sha": branch_name, "since": latest_commit_date.isoformat()}
            commits = fetch_paginated_data(commits_url, params=params)
            commit_data = [
                {
                    "repository_id": repo_id,
                    "message": c["commit"]["message"],
                    "author": c["commit"]["author"]["name"],
                    "committed_at": format_date(c["commit"]["author"]["date"]),
                    "branch": branch_name,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                for c in commits if datetime.fromisoformat(format_date(c["commit"]["author"]["date"])) > latest_commit_date
            ]

            if commit_data:
                # Fetch existing commits to deduplicate
                existing_commits = supabase.table("commits")\
                    .select("message, author, committed_at, branch")\
                    .eq("repository_id", repo_id)\
                    .execute()
                existing_keys = {(c["message"], c["author"], c["committed_at"], c["branch"]) for c in existing_commits.data}

                # Filter out duplicates
                new_commit_data = [
                    commit for commit in commit_data
                    if (commit["message"], commit["author"], commit["committed_at"], commit["branch"]) not in existing_keys
                ]

                if new_commit_data:
                    supabase.table("commits").insert(new_commit_data, upsert=True).execute()
                    print(f"Stored {len(new_commit_data)} new commits for branch {branch_name}")

        # Fetch PRs (open and closed)
        print("Fetching pull requests...")
        pr_states = ["open", "closed"]
        for state in pr_states:
            prs_url = f"{base_url}/pulls"
            params = {"state": state, "since": latest_pr_date.isoformat()}
            prs = fetch_paginated_data(prs_url, params=params)
            pr_data = [
                {
                    "repository_id": repo_id,
                    "title": p["title"],
                    "author": p["user"]["login"],
                    "created_at": format_date(p["created_at"]),
                    "state": p["state"],
                    "number": p["number"],
                    "created_at_internal": datetime.now(timezone.utc).isoformat()
                }
                for p in prs if datetime.fromisoformat(format_date(p["created_at"])) > latest_pr_date
            ]
            if pr_data:
                supabase.table("pull_requests").insert(pr_data, upsert=True).execute()
                print(f"Stored {len(pr_data)} new {state} PRs")

        # Fetch issues
        print("Fetching issues...")
        issues_url = f"{base_url}/issues"
        params = {"since": latest_issue_date.isoformat()}
        issues = fetch_paginated_data(issues_url, params=params)
        issue_data = [
            {
                "repository_id": repo_id,
                "title": i["title"],
                "author": i["user"]["login"],
                "created_at": format_date(i["created_at"]),
                "number": i["number"]
            }
            for i in issues if "pull_request" not in i and datetime.fromisoformat(format_date(i["created_at"])) > latest_issue_date
        ]
        if issue_data:
            supabase.table("issues").insert(issue_data, upsert=True).execute()
            print(f"Stored {len(issue_data)} new issues")

        # Fetch reviews for all PRs
        print("Fetching reviews...")
        prs_url = f"{base_url}/pulls?state=all&since={latest_review_date.isoformat()}"
        all_prs = fetch_paginated_data(prs_url)
        for pr in all_prs:
            reviews_url = f"{base_url}/pulls/{pr['number']}/reviews"
            params = {"since": latest_review_date.isoformat()}
            pr_reviews = fetch_paginated_data(reviews_url, params=params)
            review_data = [
                {
                    "repository_id": repo_id,
                    "comment": r["body"] if r["body"] else "No comment",
                    "author": r["user"]["login"],
                    "created_at": format_date(r["submitted_at"]),
                    "review_id": str(r["id"]),  # Ensure string conversion for text type
                    "pr_number": pr["number"]
                }
                for r in pr_reviews if datetime.fromisoformat(format_date(r["submitted_at"])) > latest_review_date
            ]
            if review_data:
                supabase.table("reviews").insert(review_data, upsert=True).execute()
                print(f"Stored {len(review_data)} new reviews for PR #{pr['number']}")

        print(f"✅ Processed {repo_name}")

    except Exception as e:
        print(f"❌ Error processing {repo_name}: {str(e)}")

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    repos = load_repositories()
    for repo in repos:
        store_repository_data(repo)

if __name__ == "__main__":
    main()