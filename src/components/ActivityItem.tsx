import React from 'react';
import { User, GitCommit, GitPullRequest, AlertCircle } from 'lucide-react';

interface ActivityItemProps {
  type: 'commit' | 'pr' | 'issue';
  title: string;
  author: string;
}

const icons = {
  commit: GitCommit,
  pr: GitPullRequest,
  issue: AlertCircle,
};

export function ActivityItem({ type, title, author }: ActivityItemProps) {
  const Icon = icons[type];
  
  return (
    <div className="flex items-start space-x-3 p-3 hover:bg-gray-50 rounded-md transition-colors">
      <div className="flex-shrink-0">
        <Icon className="w-5 h-5 text-gray-500" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium text-gray-900 truncate">{title}</p>
        <div className="flex items-center mt-1">
          <User className="w-4 h-4 text-gray-400 mr-1" />
          <p className="text-sm text-gray-500">{author}</p>
        </div>
      </div>
    </div>
  );
}
