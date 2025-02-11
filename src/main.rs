use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;

const PG_SITE: &str = "https://www.postgresql.org";

const MONTHS: &[&str] = &[
    "Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sep.", "Oct.", "Nov.", "Dec.",
];

#[derive(Debug)]
struct EmailThread {
    subject: String,
    id: String,
    datetime: NaiveDateTime,
    author: String,
}

impl std::fmt::Display for EmailThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Thread: {}\nAuthor: {}\nTime: {}\nURL: {PG_SITE}/message-id/{}",
            self.subject,
            self.author,
            self.datetime.format("%Y-%m-%d %H:%M:%S"),
            self.id
        )
    }
}

fn handle_table(
    table: &scraper::ElementRef,
    date: NaiveDate,
    seen_subjects: &mut HashSet<String>,
    threads: &mut Vec<EmailThread>,
) {
    let tr_selector = Selector::parse("tr").unwrap();
    let th_selector = Selector::parse("th").unwrap();
    let td_selector = Selector::parse("td").unwrap();
    let a_selector = Selector::parse("a").unwrap();

    for tr in table.select(&tr_selector) {
        // Get the thread subject from th
        let subject_th = tr.select(&th_selector).next();
        // Get author and time from td
        let tds: Vec<_> = tr.select(&td_selector).collect();

        // Skip table header rows
        if tds.len() == 0 {
            continue;
        }

        if let (Some(subject_td), true) = (subject_th, tds.len() >= 2) {
            let author_td = &tds[0];
            let time_td = &tds[1];

            // Get subject and URL
            if let Some(a) = subject_td.select(&a_selector).next() {
                let text = a.text().collect::<String>().trim().to_string();
                // this is a hack! Find a better way!
                let is_reply = text.starts_with("Re:")
                    || text.starts_with("rE:")
                    || text.starts_with("RE:")
                    || text.starts_with("re:");
                if !is_reply {
                    if let Some(href) = a.value().attr("href") {
                        // Clean subject
                        let clean_subject =
                            text.split('📎').next().unwrap_or(&text).trim().to_string();

                        if !seen_subjects.contains(&clean_subject) {
                            // Get author
                            let author = author_td.text().collect::<String>().trim().to_string();

                            // Get time and combine with date
                            let time_str = time_td.text().collect::<String>().trim().to_string();
                            let datetime_str = format!("{} {}", date.format("%Y-%m-%d"), time_str);
                            if let Ok(datetime) =
                                NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M")
                            {
                                seen_subjects.insert(clean_subject.clone());
                                threads.push(EmailThread {
                                    subject: clean_subject,
                                    id: href.trim_start_matches("/message-id/").to_string(),
                                    datetime,
                                    author,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

fn get_first_emails_in_one_page(url: &str) -> Result<Vec<EmailThread>> {
    let client = Client::new();
    let response = client.get(url).send().context("Failed to fetch the page")?;
    let body = response.text().context("Failed to get response text")?;

    let document = Html::parse_document(&body);
    let mut seen_subjects = HashSet::new();
    let mut threads = Vec::new();

    // Find all elements
    let h2_selector = Selector::parse("h2").unwrap();
    // Next to h2, find table
    let table_selector = Selector::parse("h2 + table").unwrap();
    let mut table_iter = document.select(&table_selector);

    // First find the date
    for h2 in document.select(&h2_selector) {
        let date_text = h2.text().collect::<String>();
        println!("Date text: {}", date_text);
        if MONTHS.contains(&date_text.split(' ').next().unwrap_or(&date_text)) {
            if let Ok(date) = NaiveDate::parse_from_str(&date_text, "%b. %d, %Y") {
                table_iter.next().and_then(|table| -> Option<()> {
                    handle_table(&table, date, &mut seen_subjects, &mut threads);
                    Some(())
                });
            }
        }
    }

    // Sort threads by datetime (newest first) and then by subject
    threads.sort_by(|a, b| b.datetime.cmp(&a.datetime).then(a.subject.cmp(&b.subject)));
    Ok(threads)
}

fn main() -> Result<()> {
    let current_url = format!("{PG_SITE}/list/pgsql-hackers/since/202402010000");

    println!("Fetching emails from: {}", current_url);
    let thread_emails = get_first_emails_in_one_page(&current_url)?;

    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
    }

    Ok(())
}
