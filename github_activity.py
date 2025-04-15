import os
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
import requests
import time
import sys
from concurrent.futures import ThreadPoolExecutor

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
    """Safely format a date string to ISO format with second precision."""
    try:
        if not date_str:
            return datetime.now(timezone.utc).isoformat()
        clean_date = date_str.replace('Z', '+00:00')
        parsed_date = datetime.fromisoformat(clean_date)
        # Truncate to seconds to avoid millisecond mismatches
        return parsed_date.replace(microsecond=0).isoformat()
    except (ValueError, TypeError):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

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
        print(f"Latest {table_name} date for repo_id {repo_id}: {result.data[0][date_field]}")
        return datetime.fromisoformat(result.data[0][date_field])
    print(f"No {table_name} data for repo_id {repo_id}, using 30 days ago: {datetime.now(timezone.utc) - timedelta(days=30)}")
    return datetime.now(timezone.utc) - timedelta(days=30)

def check_rate_limit():
    """Check GitHub API rate limit and return remaining requests and reset time."""
    response = requests.get("https://api.github.com/rate_limit", headers=HEADERS)
    if response.status_code == 200:
        remaining = response.json()["resources"]["core"]["remaining"]
        reset_time = response.json()["resources"]["core"]["reset"]
        return remaining, reset_time
    return None, None

def handle_rate_limit():
    """Check rate limit and wait if necessary with exponential backoff."""
    remaining, reset_time = check_rate_limit()
    if remaining is not None and remaining < 20:  # Buffer to avoid hitting zero
        wait_time = max(reset_time - time.time(), 0) + 5  # Add 5 second buffer
        print(f"Rate limit low ({remaining} remaining). Waiting {wait_time:.0f} seconds...")
        time.sleep(wait_time)
        return True
    return False

def fetch_paginated_data(url, params=None, max_retries=5):
    items = []
    page = 1
    retry_count = 0
    retry_delay = 5  # Initial retry delay in seconds
    
    while True:
        if params is None:
            params = {}
        params["page"] = page
        params["per_page"] = 100
        
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            print(f"Rate limit remaining: {response.headers.get('X-RateLimit-Remaining', 'N/A')}")
            
            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers and int(response.headers['X-RateLimit-Remaining']) == 0:
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                wait_time = max(reset_time - time.time(), 0) + 5  # Add buffer
                print(f"Rate limit reached. Waiting {wait_time:.0f} seconds...")
                time.sleep(wait_time)
                continue
                
            if response.status_code != 200:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"Max retries reached for {url}. Last status: {response.status_code}")
                    break
                    
                print(f"Error fetching {url}: {response.status_code}, retry {retry_count}/{max_retries} in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
                
            new_items = response.json()
            print(f"Fetched {len(new_items)} items from {url}, page {page}")
            
            if not new_items:
                print(f"Completed {page} pages for {url}")
                break
                
            items.extend(new_items)
            
            if len(new_items) < 100:
                print(f"Completed {page} pages for {url}")
                break
                
            page += 1
            retry_count = 0  # Reset retry count on successful request
            retry_delay = 5  # Reset delay
            
        except Exception as e:
            retry_count += 1
            if retry_count > max_retries:
                print(f"Max retries reached for {url} due to exception: {str(e)}")
                break
                
            print(f"Exception fetching {url}: {str(e)}, retry {retry_count}/{max_retries} in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
            
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

def clean_duplicate_commits(repo_id: str):
    """Remove duplicate commits from the table, keeping the latest entry."""
    print(f"Cleaning duplicate commits for repo_id: {repo_id}")
    # Fetch all commits
    all_commits = supabase.table("commits")\
        .select("id, repository_id, message, author, committed_at, branch, created_at")\
        .eq("repository_id", repo_id)\
        .execute()
    
    if not all_commits.data:
        print("No commits to clean")
        return

    # Group commits by unique key
    commit_groups = {}
    for commit in all_commits.data:
        key = (commit["repository_id"], commit["message"], commit["author"], commit["committed_at"], commit["branch"])
        if key not in commit_groups:
            commit_groups[key] = []
        commit_groups[key].append(commit)

    # Identify duplicates and keep the latest entry based on created_at
    for key, commits in commit_groups.items():
        if len(commits) > 1:
            # Sort by created_at (descending) to keep the most recent
            commits.sort(key=lambda x: datetime.fromisoformat(x["created_at"]), reverse=True)
            # Keep the first (most recent) and delete the rest
            commits_to_delete = commits[1:]
            for commit in commits_to_delete:
                supabase.table("commits").delete().eq("id", commit["id"]).execute()
                print(f"Deleted duplicate commit: {key}, id: {commit['id']}")

def clean_duplicate_reviews(repo_id: str):
    """Remove duplicate reviews from the table, keeping the latest entry."""
    print(f"Cleaning duplicate reviews for repo_id: {repo_id}")
    # Fetch all reviews
    all_reviews = supabase.table("reviews")\
        .select("id, repository_id, review_id, author, created_at, pr_number")\
        .eq("repository_id", repo_id)\
        .execute()
    
    if not all_reviews.data:
        print("No reviews to clean")
        return

    # Group reviews by unique key
    review_groups = {}
    for review in all_reviews.data:
        key = (review["repository_id"], review["review_id"], review["author"], review["pr_number"])
        if key not in review_groups:
            review_groups[key] = []
        review_groups[key].append(review)

    # Identify duplicates and keep the latest entry based on created_at
    for key, reviews in review_groups.items():
        if len(reviews) > 1:
            # Sort by created_at (descending) to keep the most recent
            reviews.sort(key=lambda x: datetime.fromisoformat(x["created_at"]), reverse=True)
            # Keep the first (most recent) and delete the rest
            reviews_to_delete = reviews[1:]
            for review in reviews_to_delete:
                supabase.table("reviews").delete().eq("id", review["id"]).execute()
                print(f"Deleted duplicate review: {key}, id: {review['id']}")

def clean_duplicate_prs(repo_id: str):
    """Remove duplicate PRs from the table, keeping the latest entry."""
    print(f"Cleaning duplicate PRs for repo_id: {repo_id}")
    # Fetch all PRs
    all_prs = supabase.table("pull_requests")\
        .select("id, repository_id, number, created_at_internal")\
        .eq("repository_id", repo_id)\
        .execute()
    
    if not all_prs.data:
        print("No PRs to clean")
        return

    # Group PRs by unique key
    pr_groups = {}
    for pr in all_prs.data:
        key = (pr["repository_id"], pr["number"])
        if key not in pr_groups:
            pr_groups[key] = []
        pr_groups[key].append(pr)

    # Identify duplicates and keep the latest entry based on created_at_internal
    for key, prs in pr_groups.items():
        if len(prs) > 1:
            # Sort by created_at_internal (descending) to keep the most recent
            prs.sort(key=lambda x: datetime.fromisoformat(x["created_at_internal"]), reverse=True)
            # Keep the first (most recent) and delete the rest
            prs_to_delete = prs[1:]
            for pr in prs_to_delete:
                supabase.table("pull_requests").delete().eq("id", pr["id"]).execute()
                print(f"Deleted duplicate PR: {key}, id: {pr['id']}")

def fetch_commits_for_branch(branch, base_url, latest_commit_date, repo_id):
    commits_url = f"{base_url}/commits"
    since_date = min(latest_commit_date, datetime.now(timezone.utc) - timedelta(days=30))
    params = {"sha": branch["name"], "since": since_date.isoformat()}
    commits = fetch_paginated_data(commits_url, params=params)
    return [
        {
            "repository_id": repo_id,
            "message": c["commit"]["message"],
            "author": c["commit"]["author"]["name"],
            "committed_at": format_date(c["commit"]["author"]["date"]),
            "branch": branch["name"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        for c in commits if datetime.fromisoformat(format_date(c["commit"]["author"]["date"])) >= since_date
    ]

def fetch_reviews_for_pr(pr, base_url, repo_id):
    """Fetch reviews for a PR with duplicate prevention."""
    reviews_url = f"{base_url}/pulls/{pr['number']}/reviews"
    pr_reviews = fetch_paginated_data(reviews_url)
    
    # Get existing reviews for this PR to avoid duplicates
    existing_reviews = supabase.table("reviews")\
        .select("review_id")\
        .eq("repository_id", repo_id)\
        .eq("pr_number", pr["number"])\
        .execute()
    
    existing_review_ids = {str(review["review_id"]) for review in existing_reviews.data}
    
    # Filter out reviews that already exist in database
    new_reviews = [r for r in pr_reviews if str(r["id"]) not in existing_review_ids]
    
    reviews_data = [
        {
            "repository_id": repo_id,
            "comment": r["body"] if r["body"] else "No comment",
            "author": r["user"]["login"],
            "created_at": format_date(r["submitted_at"]),
            "review_id": str(r["id"]),
            "pr_number": pr["number"]
        }
        for r in new_reviews
    ]
    
    print(f"Fetched {len(reviews_data)} new reviews for PR #{pr['number']} (filtered from {len(pr_reviews)} total)")
    return reviews_data

def store_repository_data(repo_name):
    print(f"Processing {repo_name}...")
    base_url = f"https://api.github.com/repos/{repo_name}"
    try:
        repo_id = get_or_create_repository(repo_name)
        print(f"Repository ID: {repo_id}")

        # Clean up old data
        delete_old_data(repo_id)

        # Clean up existing duplicates
        clean_duplicate_commits(repo_id)
        clean_duplicate_reviews(repo_id)
        clean_duplicate_prs(repo_id)

        # Get latest dates or default to 30 days ago
        latest_commit_date = get_latest_date("commits", repo_id, "committed_at")
        latest_pr_date = get_latest_date("pull_requests", repo_id, "created_at")
        latest_issue_date = get_latest_date("issues", repo_id, "created_at")
        # For reviews, we'll use PR date as a proxy
        latest_review_date = get_latest_date("reviews", repo_id, "created_at")

        # Fetch commits from all branches in parallel
        print(f"Starting commits at {time.time()}")
        branches_url = f"{base_url}/branches"
        branches = fetch_paginated_data(branches_url)
        print(f"Processing branches: {[b['name'] for b in branches]}")
        with ThreadPoolExecutor(max_workers=5) as executor:
            commit_data_list = list(executor.map(lambda b: fetch_commits_for_branch(b, base_url, latest_commit_date, repo_id), branches))
        commit_data = [item for sublist in commit_data_list for item in sublist]
        if commit_data:
            # Fetch all existing commits for this repository to check duplicates
            existing_commits = supabase.table("commits")\
                .select("repository_id, message, author, committed_at, branch")\
                .eq("repository_id", repo_id)\
                .execute()
            existing_keys = {(c["repository_id"], c["message"], c["author"], c["committed_at"], c["branch"]) for c in existing_commits.data}
            new_commit_data = []
            for commit in commit_data:
                key = (commit["repository_id"], commit["message"], commit["author"], commit["committed_at"], commit["branch"])
                if key not in existing_keys:
                    new_commit_data.append(commit)
                    existing_keys.add(key)  # Update existing_keys to prevent duplicates in this run
                else:
                    print(f"Skipping duplicate commit: {key}")
            if new_commit_data:
                try:
                    # Insert new commits
                    supabase.table("commits").insert(new_commit_data).execute()
                    print(f"Stored {len(new_commit_data)} new commits at {time.time()}")
                except Exception as e:
                    print(f"Error inserting commits: {e}")
                    # Insert one by one as fallback
                    successful_inserts = 0
                    for commit in new_commit_data:
                        try:
                            supabase.table("commits").insert(commit).execute()
                            successful_inserts += 1
                        except Exception as inner_e:
                            print(f"Error inserting individual commit: {inner_e}")
                    print(f"Individually inserted {successful_inserts}/{len(new_commit_data)} commits")
        print(f"Finished commits at {time.time()}")

        # Fetch PRs (open and closed)
        print(f"Starting PRs at {time.time()}")
        pr_states = ["open", "closed"]
        for state in pr_states:
            prs_url = f"{base_url}/pulls"
            since_date = min(latest_pr_date, datetime.now(timezone.utc) - timedelta(days=30))
            # Note: GitHub API doesn't support "since" for pulls directly, so we filter after fetching
            params = {"state": state, "sort": "updated", "direction": "desc"}
            prs = fetch_paginated_data(prs_url, params=params)
            
            # Filter PRs by date on our side
            filtered_prs = [p for p in prs if datetime.fromisoformat(format_date(p["created_at"])) >= since_date]
            print(f"Filtered from {len(prs)} to {len(filtered_prs)} {state} PRs based on date")
            
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
                for p in filtered_prs
            ]
            if pr_data:
                # Check for existing PRs to prevent duplicates
                existing_prs = supabase.table("pull_requests")\
                    .select("repository_id, number")\
                    .eq("repository_id", repo_id)\
                    .in_("number", [p["number"] for p in pr_data])\
                    .execute()
                existing_pr_numbers = {pr["number"] for pr in existing_prs.data}
                new_pr_data = [pr for pr in pr_data if pr["number"] not in existing_pr_numbers]
                if new_pr_data:
                    try:
                        supabase.table("pull_requests").insert(new_pr_data).execute()
                        print(f"Stored {len(new_pr_data)} new {state} PRs")
                    except Exception as e:
                        print(f"Error inserting {state} PRs: {e}")
                        # Insert one by one as fallback
                        successful_inserts = 0
                        for pr in new_pr_data:
                            try:
                                supabase.table("pull_requests").insert(pr).execute()
                                successful_inserts += 1
                            except Exception as inner_e:
                                print(f"Error inserting individual PR: {inner_e}")
                        print(f"Individually inserted {successful_inserts}/{len(new_pr_data)} {state} PRs")
                else:
                    print(f"No new {state} PRs to store after filtering duplicates")
            else:
                print(f"No {state} PRs to store after date filtering")
        print(f"Finished PRs at {time.time()}")

        # Fetch issues
        print(f"Starting issues at {time.time()}")
        since_date = min(latest_issue_date, datetime.now(timezone.utc) - timedelta(days=30))
        issues_url = f"{base_url}/issues"
        params = {"sort": "updated", "direction": "desc", "since": since_date.isoformat()}
        issues = fetch_paginated_data(issues_url, params=params)
        issue_data = [
            {
                "repository_id": repo_id,
                "title": i["title"],
                "author": i["user"]["login"],
                "created_at": format_date(i["created_at"]),
                "number": i["number"]
            }
            for i in issues if "pull_request" not in i
        ]
        if issue_data:
            # Check for existing issues to prevent duplicates
            existing_issues = supabase.table("issues")\
                .select("repository_id, number")\
                .eq("repository_id", repo_id)\
                .in_("number", [i["number"] for i in issue_data])\
                .execute()
            existing_issue_numbers = {issue["number"] for issue in existing_issues.data}
            new_issue_data = [issue for issue in issue_data if issue["number"] not in existing_issue_numbers]
            if new_issue_data:
                try:
                    supabase.table("issues").insert(new_issue_data).execute()
                    print(f"Stored {len(new_issue_data)} new issues")
                except Exception as e:
                    print(f"Error inserting issues: {e}")
                    # Insert one by one as fallback
                    successful_inserts = 0
                    for issue in new_issue_data:
                        try:
                            supabase.table("issues").insert(issue).execute()
                            successful_inserts += 1
                        except Exception as inner_e:
                            print(f"Error inserting individual issue: {inner_e}")
                    print(f"Individually inserted {successful_inserts}/{len(new_issue_data)} issues")
            else:
                print("No new issues to store after filtering duplicates")
        else:
            print("No issues to store after date filtering")
        print(f"Finished issues at {time.time()}")

        # For reviews, only fetch from PRs updated in the last 30 days
        print(f"Starting reviews at {time.time()}")
        since_date = min(latest_review_date, datetime.now(timezone.utc) - timedelta(days=30))
        
        # Get recent PRs (both open and recently closed)
        recent_prs_open = fetch_paginated_data(f"{base_url}/pulls?state=open&sort=updated&direction=desc")
        recent_prs_closed = fetch_paginated_data(f"{base_url}/pulls?state=closed&sort=updated&direction=desc")
        
        # Filter closed PRs to only those updated in the last 30 days
        filtered_prs_closed = [
            pr for pr in recent_prs_closed 
            if datetime.fromisoformat(format_date(pr["updated_at"])) >= since_date
        ]
        
        all_recent_prs = recent_prs_open + filtered_prs_closed
        print(f"Found {len(all_recent_prs)} recent PRs for review processing " +
              f"({len(recent_prs_open)} open, {len(filtered_prs_closed)} recently closed)")
        
        # Process reviews in smaller batches to manage rate limits
        batch_size = 5
        all_review_data = []
        
        for i in range(0, len(all_recent_prs), batch_size):
            batch_prs = all_recent_prs[i:i+batch_size]
            print(f"Processing reviews for PR batch {i//batch_size + 1}/{(len(all_recent_prs)-1)//batch_size + 1} " + 
                  f"(PRs #{[pr['number'] for pr in batch_prs]})")
            
            # Check rate limit before processing batch
            handle_rate_limit()
            
            # Process batch
            with ThreadPoolExecutor(max_workers=5) as executor:
                batch_reviews = list(executor.map(lambda pr: fetch_reviews_for_pr(pr, base_url, repo_id), batch_prs))
            
            # Flatten and add to review_data
            for reviews in batch_reviews:
                if reviews:
                    all_review_data.extend(reviews)
        
        print(f"Total new reviews fetched across all PRs: {len(all_review_data)}")
        
        if all_review_data:
            # Insert in smaller batches
            insert_batch_size = 20
            for j in range(0, len(all_review_data), insert_batch_size):
                batch = all_review_data[j:j + insert_batch_size]
                batch_num = j//insert_batch_size + 1
                total_batches = (len(all_review_data)-1)//insert_batch_size + 1 if len(all_review_data) > 0 else 0
                print(f"Inserting review batch {batch_num}/{total_batches} with {len(batch)} reviews")
                
                try:
                    supabase.table("reviews").insert(batch).execute()
                    print(f"Successfully inserted review batch {batch_num}/{total_batches}")
                except Exception as e:
                    print(f"Error inserting review batch {batch_num}: {e}")
                    # Try one by one as fallback
                    successful_inserts = 0
                    for review in batch:
                        try:
                            supabase.table("reviews").insert(review).execute()
                            successful_inserts += 1
                        except Exception as inner_e:
                            print(f"Error inserting individual review: {inner_e}")
                    print(f"Individually inserted {successful_inserts}/{len(batch)} reviews")
        else:
            print("No new reviews to store")
        
        print(f"Finished reviews at {time.time()}")
        print(f"✅ Processed {repo_name}")

    except Exception as e:
        print(f"❌ Error processing {repo_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    repos = load_repositories()
    for repo in repos:
        store_repository_data(repo)

if __name__ == "__main__":
    main()