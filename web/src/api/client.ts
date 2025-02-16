import axios from 'axios';
import { format } from 'date-fns';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000/api';

export interface EmailThreadDetail {
  id: string;
  subject: string;
  datetime: string;
  author_name: string;
  author_email: string;
  content: string;
}

export const getActiveSubjects = async (startDate: Date, endDate: Date, signal?: AbortSignal): Promise<EmailThreadDetail[]> => {
  const params = {
    start_date: format(startDate, 'yyyyMMddHHmmss'),
    end_date: format(endDate, 'yyyyMMddHHmmss'),
  };
  
  const response = await axios.get(`${API_BASE_URL}/active-subjects`, { params, signal });
  return response.data;
};

export const getNewSubjects = async (startDate: Date, endDate: Date, signal?: AbortSignal): Promise<EmailThreadDetail[]> => {
  const params = {
    start_date: format(startDate, 'yyyyMMddHHmmss'),
    end_date: format(endDate, 'yyyyMMddHHmmss'),
  };
  
  const response = await axios.get(`${API_BASE_URL}/new-subjects`, { params, signal });
  
  // Transform EmailThread to EmailThreadDetail
  return response.data.map((thread: { id: string; subject: string; datetime: string; author: string }) => ({
    id: thread.id,
    subject: thread.subject,
    datetime: thread.datetime,
    author_name: thread.author,
    author_email: '', // Empty string as it's not available in new subjects
    content: ''  // Empty string as it's not available in new subjects
  }));
};
