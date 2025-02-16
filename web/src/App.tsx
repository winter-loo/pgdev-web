import React, { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, CircularProgress, Button, Link, Box, Stack } from '@mui/material';
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

interface TimeRangeSection {
  id: string;
  title: string;
  currentDateRange: { startDate: Date; endDate: Date };
  onSelect: () => void;
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
  isSelected: boolean;
}

const TimeRangeSection = ({ 
  title, 
  onSelect, 
  onTimeRangeSelect,
  currentDateRange,
  isSelected
}: TimeRangeSection) => (
  <div>
    <Typography 
      variant="h6" 
      gutterBottom 
      sx={{ 
        cursor: 'pointer',
        color: isSelected ? '#1976d2' : 'inherit'
      }} 
      onClick={onSelect}
    >
      {title}
    </Typography>
    {timeRanges.map((range, index) => (
      <Button 
        key={range.label} 
        fullWidth 
        sx={{ 
          justifyContent: 'flex-start', 
          mb: index === timeRanges.length - 1 ? 3 : 1,
          color: isTimeRangeEqual(range.getRange(), currentDateRange) ? '#1976d2' : 'inherit',
          display: !isSelected ? 'none' : 'flex'
        }}
        onClick={() => onTimeRangeSelect(range.getRange())}
      >
        {range.label}
      </Button>
    ))}
  </div>
);

const NewSubjectsSection = ({
  isSelected,
  onSelect
}: {
  isSelected?: boolean;
  onSelect?: () => void;
}) => {
  const [currentDateRange, setCurrentDateRange] = useState<{ startDate: Date; endDate: Date }>({
    startDate: startOfToday(),
    endDate: endOfToday()
  });

  return (
    <TimeRangeSection
      id="new"
      title="New"
      currentDateRange={currentDateRange}
      onTimeRangeSelect={setCurrentDateRange}
      isSelected={isSelected}
      onSelect={onSelect}
    />
  );
};

const ActiveSubjectsSection = ({
  isSelected,
  onSelect
}: {
  isSelected?: boolean;
  onSelect?: () => void;
}) => {
  const [currentDateRange, setCurrentDateRange] = useState<{ startDate: Date; endDate: Date }>({
    startDate: startOfWeek(new Date()),
    endDate: endOfWeek(new Date())
  });

  return (
    <TimeRangeSection
      id="active"
      title="Active"
      currentDateRange={currentDateRange}
      onTimeRangeSelect={setCurrentDateRange}
      isSelected={isSelected}
      onSelect={onSelect}
    />
  );
};

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

// Helper function to compare time ranges
const isTimeRangeEqual = (range1: { startDate: Date; endDate: Date }, range2: { startDate: Date; endDate: Date }) => {
  return range1.startDate.getTime() === range2.startDate.getTime() &&
         range1.endDate.getTime() === range2.endDate.getTime();
};

interface NavigationItemProps {
  id: string;
  isSelected: boolean;
  onSelect: (id: string) => void;
  children: React.ReactNode;
}

const NavigationItem = ({
  id,
  isSelected,
  onSelect,
  children
}: NavigationItemProps) => {
  const handleSelect = () => {
    onSelect(id);
  };

  return (
    <Box onClick={handleSelect}>
      {React.Children.map(children, child => {
        if (React.isValidElement(child)) {
          return React.cloneElement(child, { isSelected, onSelect: handleSelect });
        }
        return child;
      })}
    </Box>
  );
};

const NavigationPanel = ({ 
  onSectionSelect,
  selectedSection
}: { 
  onSectionSelect: (section: string) => void;
  selectedSection: string;
}) => (
  <Paper sx={{ p: 2 }}>
    <Stack spacing={3}>
      <NavigationItem
        id="new"
        isSelected={selectedSection === 'new'}
        onSelect={onSectionSelect}
      >
        <NewSubjectsSection />
      </NavigationItem>
      <NavigationItem
        id="active"
        isSelected={selectedSection === 'active'}
        onSelect={onSectionSelect}
      >
        <ActiveSubjectsSection />
      </NavigationItem>
    </Stack>
  </Paper>
);

function App() {
  const [selectedSection, setSelectedSection] = useState<string>('new');
  const [activeSubjects, setActiveSubjects] = useState<EmailThreadDetail[]>([]);
  const [newSubjects, setNewSubjects] = useState<EmailThreadDetail[]>([]);
  const [loadingActive, setLoadingActive] = useState(true);
  const [loadingNew, setLoadingNew] = useState(true);
  
  // Separate date ranges for each section with different defaults
  const [activeDateRange, setActiveDateRange] = useState(() => 
    timeRanges.find(r => r.label === 'today')!.getRange()
  );
  const [newDateRange, setNewDateRange] = useState(() => 
    timeRanges.find(r => r.label === 'this week')!.getRange()
  );

  const handleTimeRangeSelect = (range: { startDate: Date; endDate: Date }) => {
    if (selectedSection === 'active') {
      setActiveDateRange(range);
    } else {
      setNewDateRange(range);
    }
  };

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    if (selectedSection === 'active') {
      setLoadingActive(true);
      console.log("fetching active subjects");
      getActiveSubjects(activeDateRange.startDate, activeDateRange.endDate)
        .then(setActiveSubjects)
        .catch(error => console.error('Error fetching active subjects:', error))
        .finally(() => setLoadingActive(false));
    } else {
      setLoadingNew(true);
      console.log("fetching new subjects");
      getNewSubjects(newDateRange.startDate, newDateRange.endDate)
        .then(setNewSubjects)
        .catch(error => console.error('Error fetching new subjects:', error))
        .finally(() => setLoadingNew(false));
    }

    return () => controller.abort();
  }, [activeDateRange, newDateRange, selectedSection]);

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      <Box sx={{ display: 'flex', gap: 2, height: 'calc(100vh - 64px)' }}>
        {/* Fixed left navigation panel */}
        <Box 
          sx={{ 
            width: '25%', 
            position: 'sticky',
            top: '64px',
            height: 'fit-content',
            maxHeight: 'calc(100vh - 96px)',
            overflowY: 'auto'
          }}
        >
          <NavigationPanel 
            onSectionSelect={setSelectedSection}
            selectedSection={selectedSection}
          />
        </Box>

        {/* Scrollable content area */}
        <Box 
          sx={{ 
            width: '75%',
            overflowY: 'auto',
            maxHeight: 'calc(100vh - 96px)'
          }}
        >
          <SubjectList
            subjects={selectedSection === 'active' ? activeSubjects : newSubjects}
            loading={selectedSection === 'active' ? loadingActive : loadingNew}
          />
        </Box>
      </Box>
    </Container>
  );
}

export default App;
