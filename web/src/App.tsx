import { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, CircularProgress, Button, Link } from '@mui/material';
import DOMPurify from 'dompurify';
import { format, startOfToday, endOfToday, startOfWeek, endOfWeek, subWeeks, startOfMonth, endOfMonth, subMonths } from 'date-fns';
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

interface TimeRange {
  label: string;
  getRange: () => { startDate: Date; endDate: Date };
}

const timeRanges: TimeRange[] = [
  {
    label: 'today',
    getRange: () => ({
      startDate: startOfToday(),
      endDate: endOfToday()
    })
  },
  {
    label: 'this week',
    getRange: () => ({
      startDate: startOfWeek(new Date(), { weekStartsOn: 1 }),
      endDate: endOfWeek(new Date(), { weekStartsOn: 1 })
    })
  },
  {
    label: 'last week',
    getRange: () => {
      const lastWeek = subWeeks(new Date(), 1);
      return {
        startDate: startOfWeek(lastWeek, { weekStartsOn: 1 }),
        endDate: endOfWeek(lastWeek, { weekStartsOn: 1 })
      };
    }
  },
  {
    label: 'this month',
    getRange: () => ({
      startDate: startOfMonth(new Date()),
      endDate: endOfMonth(new Date())
    })
  },
  {
    label: 'last month',
    getRange: () => {
      const lastMonth = subMonths(new Date(), 1);
      return {
        startDate: startOfMonth(lastMonth),
        endDate: endOfMonth(lastMonth)
      };
    }
  }
];

const Section = ({ 
  title, 
  onTitleClick, 
  onTimeRangeSelect
}: { 
  title: string; 
  onTitleClick: () => void;
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
}) => (
  <div>
    <Typography variant="h6" gutterBottom sx={{ cursor: 'pointer' }} onClick={onTitleClick}>{title}</Typography>
    {timeRanges.map((range, index) => (
      <Button 
        key={range.label} 
        fullWidth 
        sx={{ justifyContent: 'flex-start', mb: index === timeRanges.length - 1 ? 3 : 1 }}
        onClick={() => {
          onTitleClick();
          onTimeRangeSelect(range.getRange());
        }}
      >
        {range.label}
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

const NavigationPanel = ({ 
  onSectionSelect,
  onTimeRangeSelect
}: { 
  onSectionSelect: (section: 'new' | 'active') => void;
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
}) => (
  <Paper sx={{ p: 2 }}>
    <Section 
      title="New" 
      onTitleClick={() => onSectionSelect('new')}
      onTimeRangeSelect={onTimeRangeSelect}
    />
    <Section 
      title="Active" 
      onTitleClick={() => onSectionSelect('active')}
      onTimeRangeSelect={onTimeRangeSelect}
    />
  </Paper>
);

function App() {
  const [selectedSection, setSelectedSection] = useState<'new' | 'active'>('new');
  const [activeSubjects, setActiveSubjects] = useState<EmailThreadDetail[]>([]);
  const [newSubjects, setNewSubjects] = useState<EmailThreadDetail[]>([]);
  const [loadingActive, setLoadingActive] = useState(true);
  const [loadingNew, setLoadingNew] = useState(true);
  const [dateRange, setDateRange] = useState(() => timeRanges[0].getRange());

  const handleTimeRangeSelect = (range: { startDate: Date; endDate: Date }) => {
    setDateRange(range);
  };

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    if (selectedSection === 'active') {
      setLoadingActive(true);
      console.log("fetching active subjects");
      getActiveSubjects(dateRange.startDate, dateRange.endDate)
        .then(setActiveSubjects)
        .catch(error => console.error('Error fetching active subjects:', error))
        .finally(() => setLoadingActive(false));
    } else {
      setLoadingNew(true);
      console.log("fetching new subjects");
      getNewSubjects(dateRange.startDate, dateRange.endDate)
        .then(setNewSubjects)
        .catch(error => console.error('Error fetching new subjects:', error))
        .finally(() => setLoadingNew(false));
    }

    return () => controller.abort();
  }, [dateRange, selectedSection]);

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      <Grid container spacing={2}>
        <Grid xs={3}>
          <NavigationPanel 
            onSectionSelect={setSelectedSection}
            onTimeRangeSelect={handleTimeRangeSelect}
          />
        </Grid>
        <Grid xs={9}>
          <SubjectList
            subjects={selectedSection === 'active' ? activeSubjects : newSubjects}
            loading={selectedSection === 'active' ? loadingActive : loadingNew}
          />
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;
