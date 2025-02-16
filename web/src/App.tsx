import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Paper, Typography, CircularProgress, Button, Link, Box, Stack } from '@mui/material';
import DOMPurify from 'dompurify';
import { format, startOfToday, endOfToday, startOfWeek as __startOfWeek, endOfWeek as __endOfWeek, subWeeks, startOfMonth, endOfMonth, subMonths } from 'date-fns';
import { getActiveSubjects, getNewSubjects } from './api/client';
import type { EmailThreadDetail } from './api/client';
import IdGenerator from './utils/IdGenerator';

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

interface TimeRangeSectionProps {
  timeRangeIdList?: string[];
  onTimeRangeSelect: (range: { startDate: Date; endDate: Date }) => void;
}

const TimeRangeSectionComponent = ({
  timeRangeIdList,
  onTimeRangeSelect,
}: TimeRangeSectionProps) => {
  const { id, setId } = React.useContext(NavigationItemIdContext);

  return (
    <div>
      {timeRanges.map((range, index) => (
        <Button
          data-id={timeRangeIdList?.[index]}
          key={timeRangeIdList?.[index]}
          fullWidth
          sx={{
            justifyContent: 'flex-start',
            mb: index === timeRanges.length - 1 ? 3 : 1,
            color: (timeRangeIdList?.[index] === id) ? '#1976d2' : 'inherit',
          }}
          onClick={() => {
            if (id !== timeRangeIdList?.[index]) {
              onTimeRangeSelect(range.getRange());
              setId?.(timeRangeIdList?.[index] || '');
            }
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
  // could not use below statement in the useState function
  // otherwise see browser console for the error message
  const { idgen } = React.useContext(NavigationItemIdContext);
  // the lambda function is only invoked once for timeRangeIdList
  const [timeRangeIdList, _] = useState<string[]>(idgen ? timeRanges.map(() => idgen.next()) : []);

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    onWillLoad?.();
    console.log("fetching new subjects");
    getNewSubjects(currentDateRange.startDate, currentDateRange.endDate, controller.signal)
      .then((subjects) => {
        console.log("fetched new subjects");
        onDidLoad?.(subjects);
      })
      .catch(error => {
        if (!axios.isCancel(error)) {
          console.error('Error fetching new subjects:', error);
          onDidLoad?.([]);
        }
      });

    return () => controller.abort();
  }, [currentDateRange]);

  return (
    <TimeRangeSectionComponent
      timeRangeIdList={timeRangeIdList}
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
  // could not use below statement in the useState function
  // otherwise see browser console for the error message
  const { idgen } = React.useContext(NavigationItemIdContext);
  // the lambda function is only invoked once for timeRangeIdList
  const [timeRangeIdList, _] = useState<string[]>(idgen ? timeRanges.map(() => idgen.next()) : []);

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();

    onWillLoad?.();
    console.log("fetching active subjects");
    getActiveSubjects(currentDateRange.startDate, currentDateRange.endDate, controller.signal)
      .then((subjects) => {
        onDidLoad?.(subjects);
      })
      .catch(error => {
        if (!axios.isCancel(error)) {
          console.error('Error fetching active subjects:', error);
          onDidLoad?.([]);
        }
      });

    return () => controller.abort();
  }, [currentDateRange]);

  return (
    <TimeRangeSectionComponent
      timeRangeIdList={timeRangeIdList}
      onTimeRangeSelect={setCurrentDateRange}
    />
  );
};

// Helper functions
const startOfWeek = (date: Date = new Date()) => __startOfWeek(date, { weekStartsOn: 1 });
const endOfWeek = (date: Date = new Date()) => __endOfWeek(date, { weekStartsOn: 1 });

const NavigationPanel = ({
  onWillLoadSubject,
  onDidLoadSubject,
  index,
}: {
  onWillLoadSubject: () => void;
  onDidLoadSubject: (subjects: EmailThreadDetail[]) => void;
  index: string;
}) => {
  const [currentId, setCurrentId] = useState<string | null>(index);

  return (<Paper sx={{ p: 2 }}>
    <Stack spacing={3}>
      <NavigationItemIdContext.Provider value={{ idgen: new IdGenerator(), id: currentId, setId: setCurrentId }}>
        <NavigationItem title="New" expanded >
          <NewSubjectsSection onWillLoad={onWillLoadSubject} onDidLoad={onDidLoadSubject} />
        </NavigationItem>
        <NavigationItem title="Active" >
          <ActiveSubjectsSection onWillLoad={onWillLoadSubject} onDidLoad={onDidLoadSubject} />
        </NavigationItem>
      </NavigationItemIdContext.Provider>
    </Stack>
  </Paper>);
};

interface NavigationItemIdProps {
  idgen: IdGenerator | null;
  id: string | null;
  setId: (id: string) => void;
}

const NavigationItemIdContext = React.createContext<NavigationItemIdProps>({ idgen: null, id: null, setId: () => { } });

interface NavigationItemProps {
  title: string;
  expanded?: boolean;
  selected?: boolean;
  onClick?: (item: string) => void;
  children: React.ReactElement;
}

// NavigationItem outputs EmailThreadDetail[] when selected
// through onDataChange
const NavigationItem = ({
  title,
  // when selected, the user can toggle the expansion
  // when not selected, the item can keep the expansion state
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
        }}
        onClick={handleSelect}
      >
        {title}
      </Typography>
      {/* navigation item content */}
      <Box sx={{ display: isExpanded ? 'block' : 'none' }}>
        {React.isValidElement(children) && React.cloneElement(children)}
      </Box>
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
            // the default highlighted item index
            // In React dev mode, it has a bug that the same NavigationItem
            // will be loaded twice at the same time, which causes a gap
            // between the items.
            index="2"
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
