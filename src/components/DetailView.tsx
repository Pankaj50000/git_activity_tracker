import React from 'react';
import { format } from 'date-fns';
import type { ActivityItem } from '../types';
import Modal from './Modal';

interface DetailViewProps {
  type: 'commit' | 'pr' | 'issue' | 'review' | null;
  data: ActivityItem[] | null;
  onClose: () => void;
}

const DetailView: React.FC<DetailViewProps> = ({ type, data, onClose }) => {
  if (!type || !data) {
    return null;
  }

  return (
    <Modal onClose={onClose}>
      <div className="p-6">
        <h2 className="text-xl font-bold mb-4">{type === 'pr' ? 'Pull Requests' : type === 'review' ? 'Reviews' : 'Commits'}</h2>
        <ul className="overflow-y-auto max-h-80">
          {data.map((item, index) => (
            <li key={index} className="mb-2">
              <div className="flex items-center">
                <span className="text-gray-600 mr-2">{item.title}</span>
                <span className="text-gray-500">by {item.author}</span>
                <span className="text-gray-500 ml-2">{format(new Date(item.date), 'MMM d, yyyy')}</span>
              </div>
              {item.type === 'pr' && (
                <div>
                  <span className="text-gray-500">State: {item.state}</span>
                </div>
              )}
              {item.type === 'review' && (
                <div>
                  <span className="text-gray-500">PR: {item.repository}</span>
                </div>
              )}
            </li>
          ))}
        </ul>
      </div>
    </Modal>
  );
};

export default DetailView;
