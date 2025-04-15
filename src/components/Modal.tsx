import React, { useRef, useEffect } from 'react';
import ReactDOM from 'react-dom';

interface ModalProps {
  children: React.ReactNode;
  onClose: () => void;
}

const Modal: React.FC<ModalProps> = ({ children, onClose }) => {
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (modalRef.current && !modalRef.current.contains(event.target as Node)) {
        onClose();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [onClose]);

  return ReactDOM.createPortal(
    <div className="fixed inset-0 flex items-center justify-center bg-black bg-opacity-50">
      <div className="bg-white rounded-lg shadow-lg w-1/2 max-w-2xl" ref={modalRef}>
        {children}
        <button className="absolute top-4 right-4 p-2 bg-gray-200 rounded-full hover:bg-gray-300" onClick={onClose}>
          &times;
        </button>
      </div>
    </div>,
    document.getElementById('root')!
  );
};

export default Modal;
