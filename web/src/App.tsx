import React, { useState, useEffect } from 'react';
import { Container, Paper, Typography, CircularProgress, Button, Link, Box, Stack } from '@mui/material';
import DOMPurify from 'dompurify';
import { format, startOfToday, endOfToday, startOfWeek as __startOfWeek, endOfWeek as __endOfWeek, subWeeks, startOfMonth, endOfMonth, subMonths } from 'date-fns';
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
      startDate: startOfWeek(new Date()),
      endDate: endOfWeek(new Date())
    })
  },
  {
    label: 'last week',
    getRange: () => {
      const lastWeek = subWeeks(new Date(), 1);
      return {
        startDate: startOfWeek(lastWeek),
        endDate: endOfWeek(lastWeek)
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
  onSelect?: () => void;
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
  isSelected?: boolean;
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

interface TimeRangeSectionProps {
  currentDateRange: { startDate: Date; endDate: Date };
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
}

const TimeRangeSectionComponent = ({
  onTimeRangeSelect,
  currentDateRange,
}: TimeRangeSectionProps) => {
  const { navItemSelected, onNavItemSelected, navItemTitle } = React.useContext(NavigationContext);

  return (
    <div>
      {timeRanges.map((range, index) => (
        <Button
          key={range.label}
          fullWidth
          sx={{
            justifyContent: 'flex-start',
            mb: index === timeRanges.length - 1 ? 3 : 1,
            color: (navItemSelected && isTimeRangeEqual(range.getRange(), currentDateRange)) ? '#1976d2' : 'inherit',
          }}
          onClick={() => {
            onTimeRangeSelect(range.getRange());
            onNavItemSelected?.(navItemTitle || '')
          }}
        >
          {range.label}
        </Button>
      ))}
    </div>
  );
}

const NewSubjectsSection = ({
  onWillLoad,
  onDidLoad
}: {
  onWillLoad?: () => void;
  onDidLoad?: (subjects: EmailThreadDetail[]) => void;
}) => {
  const [currentDateRange, setCurrentDateRange] = useState<{ startDate: Date; endDate: Date }>({
    startDate: startOfWeek(new Date()),
    endDate: endOfWeek(new Date())
  });

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    onWillLoad?.();
    console.log("fetching new subjects");
    getNewSubjects(currentDateRange.startDate, currentDateRange.endDate)
      .then((subjects) => {
        console.log("fetched new subjects");
        onDidLoad?.(subjects);
      })
      .catch(error => {
        console.error('Error fetching new subjects:', error);
        onDidLoad?.([]);
      });

    return () => controller.abort();
  }, [currentDateRange]);

  return (
    <TimeRangeSectionComponent
      currentDateRange={currentDateRange}
      onTimeRangeSelect={setCurrentDateRange}
    />
  );
};

const ActiveSubjectsSection = ({
  onWillLoad,
  onDidLoad,
}: {
  onWillLoad?: () => void;
  onDidLoad?: (subjects: EmailThreadDetail[]) => void;
}) => {
  const [currentDateRange, setCurrentDateRange] = useState<{ startDate: Date; endDate: Date }>({
    startDate: startOfToday(),
    endDate: endOfToday()
  });

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    onWillLoad?.();
    console.log("fetching active subjects");
    getActiveSubjects(currentDateRange.startDate, currentDateRange.endDate)
      .then((subjects) => {
        onDidLoad?.(subjects);
      })
      .catch(error => {
        console.error('Error fetching active subjects:', error);
        onDidLoad?.([]);
      });

    return () => controller.abort();
  }, [currentDateRange]);

  return (
    <TimeRangeSectionComponent
      currentDateRange={currentDateRange}
      onTimeRangeSelect={setCurrentDateRange}
    />
  );
};

// Helper functions
const startOfWeek = (date: Date = new Date()) => __startOfWeek(date, { weekStartsOn: 1 });
const endOfWeek = (date: Date = new Date()) => __endOfWeek(date, { weekStartsOn: 1 });

const isTimeRangeEqual = (range1: { startDate: Date; endDate: Date }, range2: { startDate: Date; endDate: Date }) => {
  return range1.startDate.getTime() === range2.startDate.getTime() &&
    range1.endDate.getTime() === range2.endDate.getTime();
};

const NavigationPanel = ({
  onWillLoadSubject,
  onDidLoadSubject
}: {
  onWillLoadSubject: () => void;
  onDidLoadSubject: (subjects: EmailThreadDetail[]) => void;
}) => {
  const [selectedItem, setSelectedItem] = useState<string | null>("New");

  return (<Paper sx={{ p: 2 }}>
    <Stack spacing={3}>
      <NavigationItem
        title="New"
        expanded
        selected={selectedItem === 'New'}
        onClick={setSelectedItem}
      >
        <NewSubjectsSection onWillLoad={onWillLoadSubject} onDidLoad={onDidLoadSubject} />
      </NavigationItem>
      <NavigationItem
        title="Active"
        selected={selectedItem === 'Active'}
        onClick={setSelectedItem}
      >
        <ActiveSubjectsSection onWillLoad={onWillLoadSubject} onDidLoad={onDidLoadSubject} />
      </NavigationItem>
    </Stack>
  </Paper>);
};

interface NavigationContextType {
  navItemSelected?: boolean;
  onNavItemSelected?: (title: string) => void;
  navItemTitle?: string;
}

const NavigationContext = React.createContext<NavigationContextType>({ navItemSelected: false, onNavItemSelected: undefined, navItemTitle: undefined });

interface NavigationItemProps {
  title: string;
  expanded?: boolean;
  selected?: boolean;
  onClick?: (item: string) => void;
  children: React.ReactElement;
}

const NavigationItem = ({
  title,
  // when selected, we need add hint to the user
  selected,
  // when selected, the user can toggle the expansion
  expanded,
  onClick,
  children
}: NavigationItemProps) => {
  const [isExpanded, setIsExpanded] = useState(expanded);

  const handleSelect = () => {
    setIsExpanded(!isExpanded);
    onClick?.(title);
  };

  return (
    <Box>
      {/* navigation item title */}
      <Typography
        variant="h6"
        gutterBottom
        sx={{
          cursor: 'pointer',
          color: selected ? '#1976d2' : 'inherit'
        }}
        onClick={handleSelect}
      >
        {title}
      </Typography>
      {/* navigation item content */}
      <NavigationContext.Provider value={{ navItemSelected: selected, onNavItemSelected: onClick, navItemTitle: title }}>
        <Box sx={{ display: isExpanded ? 'block' : 'none' }}>
          {React.isValidElement(children) && React.cloneElement(children)}
        </Box>
      </NavigationContext.Provider>
    </Box>
  );
};

function App() {
  const [subjects, setSubjects] = useState<EmailThreadDetail[]>([]);
  const [loadingSubjects, setLoadingSubjects] = useState<boolean>(false);

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
            onWillLoadSubject={
              () => {
                setLoadingSubjects(true);
              }
            }
            onDidLoadSubject={
              (subjects: EmailThreadDetail[]) => {
                setLoadingSubjects(false);
                setSubjects(subjects);
              }
            }
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
            subjects={subjects}
            loading={loadingSubjects}
          />
        </Box>
      </Box>
    </Container>
  );
}

export default App;
