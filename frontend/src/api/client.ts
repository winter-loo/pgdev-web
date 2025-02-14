import axios from 'axios';
import { format } from 'date-fns';

const API_BASE_URL = 'http://localhost:3000/api';

export interface EmailThread {
  id: string;
  subject: string;
  datetime: string;
  author: string;
}

export interface EmailThreadDetail {
  id: string;
  subject: string;
  datetime: string;
  author_name: string;
  author_email: string;
  content: string;
}

export const getActiveSubjects = async (startDate: Date, endDate: Date): Promise<EmailThreadDetail[]> => {
  const params = {
    start_date: format(startDate, 'yyyy-MM-dd HH:mm:ss'),
    end_date: format(endDate, 'yyyy-MM-dd HH:mm:ss'),
  };
  
  const response = await axios.get(`${API_BASE_URL}/active-subjects`, { params });
  return response.data;
};

export const getNewSubjects = async (startDate: Date, endDate: Date): Promise<EmailThread[]> => {
  const params = {
    start_date: format(startDate, 'yyyy-MM-dd HH:mm:ss'),
    end_date: format(endDate, 'yyyy-MM-dd HH:mm:ss'),
  };
  
  const response = await axios.get(`${API_BASE_URL}/new-subjects`, { params });
  return response.data;
};
