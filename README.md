# pgdev-web

## how to find the very first email in a thread

### fast approach

From the page https://www.postgresql.org/list/pgsql-hackers/since/202304010000/, iterate over each thread and filter out the thread title including the word 'Re:'.

This approch is fast, but it misses some threads.


### correct approach

From the page https://www.postgresql.org/list/pgsql-hackers/since/202304010000/, iterate over each thread and enter the thread page. On that page, click the 'Whole Thread' link. This will take you to a page where all the messages in the thread are listed. The first message should be the very first email in the thread.

This approach is slower, but it finds all the very first emails in the threads.

At the 'Whole Thread' page, all message ids should be kept in memory for later reference. When iterate over other threads, use the message ids to decide whether the current thread is the very first email. If the message id is in the set of message ids, the current thread is not the very first email. Otherwise, we need enter the current thread to find the very first email.

### bold approach

combine the previous two approches
