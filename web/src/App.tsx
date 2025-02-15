import { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, CircularProgress, Button, Link } from '@mui/material';
import DOMPurify from 'dompurify';
import { format } from 'date-fns';
import { getActiveSubjects, getNewSubjects } from './api/client';
import type { EmailThreadDetail } from './api/client';

interface SubjectContentProps {
  content: string;
}

const SubjectContent = ({ content }: SubjectContentProps) => {
  const [isExpanded, setIsExpanded] = useState(false);
  if (!content) return null;

  return (
    <div>
      <Button 
        onClick={() => setIsExpanded(!isExpanded)}
        sx={{ mt: 1 }}
      >
        {isExpanded ? 'Hide Content' : 'Show Content'}
      </Button>
      {isExpanded && (
        <div 
          dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(content) }}
        />
      )}
    </div>
  );
};

const Section = ({ title, buttons, onTitleClick }: { title: string; buttons: string[]; onTitleClick: () => void }) => (
  <div>
    <Typography variant="h6" gutterBottom sx={{ cursor: 'pointer' }} onClick={onTitleClick}>{title}</Typography>
    {buttons.map((label, index) => (
      <Button key={index} fullWidth sx={{ justifyContent: 'flex-start', mb: index === buttons.length - 1 ? 3 : 1 }}>
        {label}
      </Button>
    ))}
  </div>
);

interface SubjectListProps {
  subjects: EmailThreadDetail[];
  loading: boolean;
}

const SubjectList = ({ subjects, loading }: SubjectListProps) => {
  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
        <CircularProgress />
      </div>
    );
  }

  return subjects.map((subject) => (
    <Paper key={subject.id} sx={{ p: 2, mb: 2 }}>
      <Typography variant="h6">
        <Link
          href={`https://www.postgresql.org/message-id/${subject.id}`}
          target="_blank"
          rel="noopener noreferrer"
          underline="hover"
          color="inherit"
        >
          {subject.subject}
        </Link>
      </Typography>
      <Typography variant="body2" color="text.secondary">
        By {subject.author_name} ({subject.author_email})
      </Typography>
      <Typography variant="body2" color="text.secondary">
        {format(new Date(subject.datetime), 'PPpp')}
      </Typography>
      <SubjectContent content={subject.content} />
    </Paper>
  ));
};

const NavigationPanel = ({ onSectionChange }: { onSectionChange: (section: 'new' | 'active') => void }) => (
  <Paper sx={{ p: 2 }}>
    <Section 
      title="New" 
      buttons={['today', 'this week', 'last week', 'this month', 'last month']} 
      onTitleClick={() => onSectionChange('new')}
    />
    <Section 
      title="Active" 
      buttons={['today', 'this week', 'last week', 'this month', 'last month']} 
      onTitleClick={() => onSectionChange('active')}
    />
  </Paper>
);

function App() {
  const [activeSection, setActiveSection] = useState<'new' | 'active'>('active');
  const [activeSubjects, setActiveSubjects] = useState<EmailThreadDetail[]>([]);
  const [newSubjects, setNewSubjects] = useState<EmailThreadDetail[]>([]);
  const [loadingActive, setLoadingActive] = useState(true);
  const [loadingNew, setLoadingNew] = useState(true);


  useEffect(() => {
    const controller = new AbortController();

    const fetchData = async () => {
      const endDate = new Date();
      const startDate = new Date(endDate);
      startDate.setDate(startDate.getDate() - 1);

      // Fetch active subjects
      getActiveSubjects(startDate, endDate)
        .then(setActiveSubjects)
        .catch(error => console.error('Error fetching active subjects:', error))
        .finally(() => setLoadingActive(false));

      // Fetch new subjects
      getNewSubjects(startDate, endDate)
        .then(setNewSubjects)
        .catch(error => console.error('Error fetching new subjects:', error))
        .finally(() => setLoadingNew(false));
    };

    fetchData();
    return () => controller.abort(); // Cancel request on unmount
  }, []);

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      <Grid container spacing={2}>
        <Grid xs={3}>
          <NavigationPanel onSectionChange={setActiveSection} />
        </Grid>
        <Grid xs={9}>
          <SubjectList
            subjects={activeSection === 'active' ? activeSubjects : newSubjects}
            loading={loadingActive || loadingNew}
          />
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;
