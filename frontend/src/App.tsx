import { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, CircularProgress } from '@mui/material';
import { format } from 'date-fns';
import { getActiveSubjects, getNewSubjects } from './api/client';
import type { EmailThread, EmailThreadDetail } from './api/client';

function App() {
  const [activeSubjects, setActiveSubjects] = useState<EmailThreadDetail[]>([]);
  const [newSubjects, setNewSubjects] = useState<EmailThread[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const endDate = new Date();
        const startDate = new Date(endDate);
        startDate.setDate(startDate.getDate() - 1); // Last 1 day

        const [newData] = await Promise.all([
          getNewSubjects(startDate, endDate)
        ]);

        setNewSubjects(newData);
      } catch (error) {
        console.error('Error fetching data:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) {
    return (
      <Container sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <CircularProgress />
      </Container>
    );
  }

  return (
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2 }}>
            <Typography variant="h5" gutterBottom>
              New Subjects
            </Typography>
            {newSubjects.map((subject) => (
              <Paper key={subject.id} sx={{ p: 2, mb: 2 }}>
                <Typography variant="h6">{subject.subject}</Typography>
                <Typography variant="body2" color="text.secondary">
                  By {subject.author}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {format(new Date(subject.datetime), 'PPpp')}
                </Typography>
              </Paper>
            ))}
          </Paper>
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;
