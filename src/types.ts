export interface Commit {
  author: string;
  message: string;
  date: string;  // Add date field
}

export interface PullRequest {
  title: string;
  author: string;
  created_at: string;  // Add date field
  state: string;
}

export interface Issue {
  title: string;
  author: string;
  created_at: string;  // Add date field
}

export interface Review {
  author: string;
  comment: string;
  created_at: string;  // Add date field
}

export interface RepositoryData {
  repository: string;
  commits: Commit[];
  pull_requests: PullRequest[];
  issues: Issue[];
  reviews: Review[];
}

export interface ActivityItem {
  type: 'commit' | 'pr' | 'issue' | 'review';
  title: string;
  author: string;
  date: string;
  repository: string;
  state?: string;
}
