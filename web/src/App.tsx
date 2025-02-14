import { useState, useEffect } from 'react';
import { Container, Grid, Paper, Typography, CircularProgress, Button, Link } from '@mui/material';
import DOMPurify from 'dompurify';
import { format } from 'date-fns';
import { getActiveSubjects, getNewSubjects } from './api/client';
import type { EmailThread, EmailThreadDetail } from './api/client';

function App() {
  const [activeSubjects, setActiveSubjects] = useState<EmailThreadDetail[]>([]);
  const [newSubjects, setNewSubjects] = useState<EmailThread[]>([]);
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
    <Container maxWidth="lg" sx={{ py: 4 }}>
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, minHeight: 200 }}>
            <Typography variant="h5" gutterBottom>
              Active Subjects
            </Typography>
            {loadingActive ? (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
                <CircularProgress />
              </div>
            ) : (
              activeSubjects.map((subject) => (
                <Paper key={subject.id} sx={{ p: 2, mb: 2 }}>
                  <Typography variant="h6">
                    <Link
                      href={`https://www.postgresql.com/message-id/${subject.id}`}
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
                  <div>
                    <Typography 
                      variant="body1" 
                      sx={{ 
                        mt: 1,
                        maxHeight: subject.content.length > 300 ? '100px' : 'none',
                        overflow: 'hidden',
                        position: 'relative',
                        '& .fade-overlay': {
                          display: subject.content.length > 300 ? 'block' : 'none',
                          position: 'absolute',
                          bottom: 0,
                          left: 0,
                          right: 0,
                          height: '50px',
                          background: 'linear-gradient(180deg, rgba(255,255,255,0) 0%, rgba(255,255,255,1) 100%)',
                        }
                      }} 
                      component="div"
                      dangerouslySetInnerHTML={{ 
                        __html: DOMPurify.sanitize(subject.content) 
                      }} 
                    />
                    {subject.content.length > 300 && (
                      <Button 
                        onClick={(e) => {
                          const target = e.currentTarget;
                          const contentEl = target.previousElementSibling as HTMLElement;
                          const isExpanded = contentEl.style.maxHeight === 'none';
                          contentEl.style.maxHeight = isExpanded ? '100px' : 'none';
                          target.textContent = isExpanded ? 'Show More' : 'Show Less';
                          const overlay = contentEl.querySelector('.fade-overlay') as HTMLElement;
                          if (overlay) {
                            overlay.style.display = isExpanded ? 'block' : 'none';
                          }
                        }}
                        sx={{ mt: 1 }}
                      >
                        Show More
                      </Button>
                    )}
                    <div className="fade-overlay" />
                  </div>
                </Paper>
              ))
            )}
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, minHeight: 200 }}>
            <Typography variant="h5" gutterBottom>
              New Subjects
            </Typography>
            {loadingNew ? (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '200px' }}>
                <CircularProgress />
              </div>
            ) : (
              newSubjects.map((subject) => (
                <Paper key={subject.id} sx={{ p: 2, mb: 2 }}>
                  <Typography variant="h6">
                    <Link
                      href={`https://www.postgresql.com/message-id/${subject.id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      underline="hover"
                      color="inherit"
                    >
                      {subject.subject}
                    </Link>
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    By {subject.author}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {format(new Date(subject.datetime), 'PPpp')}
                  </Typography>
                </Paper>
              ))
            )}
          </Paper>
        </Grid>
      </Grid>
    </Container>
  );
}

export default App;
