import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { Container, Paper, Typography, CircularProgress, Button, Link, Box, Stack } from '@mui/material';
import DOMPurify from 'dompurify';
import { format, startOfToday, endOfToday, startOfWeek as __startOfWeek, endOfWeek as __endOfWeek, startOfYesterday, subWeeks, startOfMonth, endOfMonth, subMonths } from 'date-fns';
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
    label: 'since yesterday',
    getRange: () => ({
      startDate: startOfYesterday(),
      endDate: new Date(),
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
  timeRanges: TimeRangeWithId[];
  onTimeRangeSelect: (range: TimeRangeWithId) => void;
}

const TimeRangeSectionComponent = ({
  timeRanges,
  onTimeRangeSelect,
}: TimeRangeSectionProps) => {
  const { id, setId } = React.useContext(NavigationItemIdContext);

  return (
    <div>
      {timeRanges.map((range, index) => (
        <Button
          data-id={timeRanges[index].id}
          key={timeRanges[index].id}
          fullWidth
          sx={{
            justifyContent: 'flex-start',
            mb: index === timeRanges.length - 1 ? 3 : 1,
            color: (timeRanges[index].id === id) ? '#1976d2' : 'inherit',
          }}
          onClick={() => {
            console.log("current selected: ", id, "clicked", timeRanges[index].id);
            // if the same item clicked again, do nothing
            if (id !== timeRanges[index].id) {
              onTimeRangeSelect(range);
              setId?.(timeRanges[index].id || '');
            }
          }}
        >
          {range.label}
        </Button>
      ))}
    </div>
  );
}

interface TimeRangeWithId {
  id?: string;
  label: string;
  getRange: () => { startDate: Date; endDate: Date };
}

const NewSubjectsSection = ({
  onWillLoad,
  onDidLoad
}: {
  onWillLoad?: () => void;
  onDidLoad?: (subjects: EmailThreadDetail[]) => void;
}) => {
  // could not use below statement in the useState function
  // otherwise see browser console for the error message
  const { id, idgen } = React.useContext(NavigationItemIdContext);
  // the lambda function is only invoked once for timeRangeIdList
  const [myTimeRanges] = useState(
    timeRanges.map((range) => ({
      id: idgen?.next(),
      ...range
    }))
  );
  const [currentDateRange, setCurrentDateRange] = useState<TimeRangeWithId>({
    ...myTimeRanges[1]
  });

  // Fetch data when date range changes
  useEffect(() => {
    const controller = new AbortController();
    // id changed but not changed to current item
    if (currentDateRange.id != id) {
      return;
    }

    onWillLoad?.();
    console.log("fetching new subjects");
    let { startDate, endDate } = currentDateRange.getRange();
    getNewSubjects(startDate, endDate, controller.signal)
      .then((subjects) => {
        console.log("fetched new subjects");
        // This condition is to avoid displaying the data loaded later from an
        // item that is not selected now.
        if (currentDateRange.id == id) {
          onDidLoad?.(subjects);
        }
      })
      .catch(error => {
        if (!axios.isCancel(error)) {
          console.error('Error fetching new subjects:', error);
          if (currentDateRange.id == id) {
            onDidLoad?.([]);
          }
        }
      });

    return () => controller.abort();
  },
  // TODO:
  // two dependencies to fix a case:
  // when active item 2 clicked and data loaded, then new item 2 clicked and data loaded,
  // then click active item 2 again, the page dost not display its data.
  // 
  // add id as dependency to fix the issue but it causes another issue:
  // each time an item is clicked, two network requests are sent.
  // fixed by checking currentDateRange.id == id, see code before.
  [currentDateRange, id]);

  return (
    <TimeRangeSectionComponent
      timeRanges={myTimeRanges}
      onTimeRangeSelect={(x) => {
        console.log(
          "new current selected: ",
          currentDateRange.id,
          "clicked",
          x.id
        );
        setCurrentDateRange(x);
      }}
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
  // could not use below statement in the useState function
  // otherwise see browser console for the error message
  const { id, idgen } = React.useContext(NavigationItemIdContext);
  // the lambda function is only invoked once for timeRangeIdList
  const [myTimeRanges] = useState(
    timeRanges.map((range) => ({
      id: idgen?.next(),
      ...range
    }))
  );
  // set to null to disallow fetching on initial page loading
  const [currentDateRange, setCurrentDateRange] = useState<TimeRangeWithId | null>(null);

  // Fetch data when date range changes
  useEffect(() => {
    // null then do not fetch data
    if (!currentDateRange) {
      return;
    }
    // id changed but not changed to current item
    if (currentDateRange.id != id) {
      return;
    }
    const controller = new AbortController();

    onWillLoad?.();
    console.log("fetching active subjects");
    let { startDate, endDate } = currentDateRange.getRange();
    getActiveSubjects(startDate, endDate, controller.signal)
      .then((subjects) => {
        // This condition is to avoid displaying the data loaded later from an
        // item that is not selected now.
        if (currentDateRange.id == id) {
          onDidLoad?.(subjects);
        }
      })
      .catch(error => {
        if (!axios.isCancel(error)) {
          console.error('Error fetching active subjects:', error);
          if (currentDateRange.id == id) {
            onDidLoad?.([]);
          }
        }
      });

    return () => controller.abort();
  },
  // two dependencies to fix a case:
  // when active item 2 clicked and data loaded, then new item 2 clicked and data loaded,
  // then click active item 2 again, the page dost not display its data.
  // 
  // add id as dependency to fix the issue but it causes another issue:
  // each time an item is clicked, two network requests are sent.
  // fixed by checking currentDateRange.id == id, see code before.
  [currentDateRange, id]);

  return (
    <TimeRangeSectionComponent
      timeRanges={myTimeRanges}
      onTimeRangeSelect={(x) => {
        console.log(
          "active current selected: ",
          currentDateRange?.id,
          "clicked",
          x.id
        );
        setCurrentDateRange(x);
      }}
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
