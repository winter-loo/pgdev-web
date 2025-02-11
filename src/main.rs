use anyhow::{Context, Ok, Result};
use chrono::{NaiveDate, NaiveDateTime};
use const_format::concatcp;
use phf::phf_map;
use reqwest::blocking::Client;
use scraper::{Html, Selector};
use std::collections::HashSet;

const PG_SITE: &str = "https://www.postgresql.org";
const WHOLE_THREAD_URL_PREFIX: &str = concatcp!(PG_SITE, "/message-id/flat");

// compile-time lookup table
static MONTHS_MAP: phf::Map<&'static str, &'static str> = phf_map! {
    "Jan." => "January",
    "Feb." => "February",
    "March" => "March",
    "April" => "April",
    "May" => "May",
    "June" => "June",
    "July" => "July",
    "Aug." => "August",
    "Sept." => "September",
    "Oct." => "October",
    "Nov." => "November",
    "Dec." => "December",
};

fn transform_date(date_text: &str) -> Option<NaiveDate> {
    let date_text: String = date_text
        .split(' ')
        .map(|s| {
            MONTHS_MAP
                .get(s)
                .map(|s| s.to_string())
                .unwrap_or(s.to_string())
        })
        .collect();
    NaiveDate::parse_from_str(&date_text, "%B %d, %Y").ok()
}

#[derive(Debug)]
struct EmailThread {
    id: String,
    subject: String,
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
    mut handle_email_thread: impl FnMut(EmailThread) -> bool,
) -> bool {
    let tr_selector = Selector::parse("tr").unwrap();
    let th_selector = Selector::parse("th").unwrap();
    let td_selector = Selector::parse("td").unwrap();
    let a_selector = Selector::parse("a").unwrap();
    let mut handle_ok = true;

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
                let clean_subject = text.split('📎').next().unwrap_or(&text).trim().to_string();
                let href = a.value().attr("href").unwrap_or("");
                let author = author_td.text().collect::<String>().trim().to_string();
                let time_str = time_td.text().collect::<String>().trim().to_string();
                let datetime_str = format!("{} {}", date.format("%Y-%m-%d"), time_str);
                let datetime = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M")
                    .unwrap_or_default();

                if !handle_email_thread(EmailThread {
                    id: href.trim_start_matches("/message-id/").to_string(),
                    subject: clean_subject,
                    datetime,
                    author,
                }) {
                    handle_ok = false;
                    break;
                }
            }
        }
    }
    handle_ok
}

fn get_document(url: &str) -> Result<Html> {
    println!("get document from {url}");
    let client = Client::new();
    let response = client.get(url).send().context("Failed to fetch the page")?;
    let body = response.text().context("Failed to get response text")?;
    println!("get document from {url}, done");

    let document = Html::parse_document(&body);
    Ok(document)
}

/// handle threads of each day but the last day in the page
fn for_each_thread(url: &str, mut handle: impl FnMut(EmailThread) -> bool) -> Result<()> {
    let document = get_document(url)?;

    // Find all elements
    let h2_selector = Selector::parse("h2").unwrap();
    // Next to h2, find table
    let table_selector = Selector::parse("h2 + table").unwrap();
    let mut table_iter = document.select(&table_selector);
    // do not process the last table
    let mut next = table_iter.next();
    let mut next2 = table_iter.next();

    // First find the date
    for h2 in document.select(&h2_selector) {
        let date_text = h2.text().collect::<String>();
        if let Some(date) = transform_date(&date_text) {
            if let Some(false) =
                next.and_then(|table| Some(handle_table(&table, date, &mut handle)))
            {
                break;
            }
            next = next2;
            next2 = table_iter.next();
            if next2.is_none() {
                break;
            }
        }
    }
    Ok(())
}

fn get_new_subjects_between(start_date: &str, end_date: &str) -> Result<Vec<EmailThread>> {
    let mut start_date =
        NaiveDate::parse_from_str(start_date, "%Y%m%d").context("parse start date")?;
    let end_date = NaiveDate::parse_from_str(end_date, "%Y%m%d").context("parse end date")?;
    let mut threads = Vec::new();
    while start_date <= end_date {
        println!("start_date={start_date:#?} end_date={end_date:#?}");
        let current_url = format!(
            "{PG_SITE}/list/pgsql-hackers/since/{}0000",
            start_date.format("%Y%m%d")
        );
        for_each_thread(&current_url, |thread| {
            let whole_thread_url = format!("{WHOLE_THREAD_URL_PREFIX}/{}", thread.id);
            start_date = thread.datetime.into();
            if is_first_email(&whole_thread_url, &thread) {
                threads.push(thread);
            }
            // we only handle threads between start_date and end_date
            start_date <= end_date
        })
        .context("Failed to process email threads")?;
        start_date = start_date.succ_opt().unwrap();
    }
    Ok(threads)
}

fn is_first_email(whole_thread_url: &str, thread: &EmailThread) -> bool {
    if thread.subject.starts_with("Re:")
        || thread.subject.starts_with("re:")
        || thread.subject.starts_with("RE:")
        || thread.subject.starts_with("rE:")
    {
        return false;
    }

    if !thread.subject.to_lowercase().contains("re:") {
        return true;
    }

    let document = get_document(whole_thread_url)
        .context("Failed to get document")
        .unwrap();
    let tag_table = Selector::parse("table.message-header").unwrap();
    let tag_tr = Selector::parse("tr").unwrap();
    let tag_td = Selector::parse("td").unwrap();
    let tag_a = Selector::parse("a").unwrap();
    let mut ans = false;
    for elem_table in document.select(&tag_table) {
        let rows = elem_table.select(&tag_tr).count();
        let mid_idx = if rows == 7 {
            4
        } else if rows == 8 {
            5
        } else {
            0
        };
        if mid_idx != 0 {
            let elem_tr = elem_table.select(&tag_tr).nth(mid_idx).unwrap();
            let elem_td = elem_tr.select(&tag_td).nth(0).unwrap();
            let elem_a = elem_td
                .select(&tag_a)
                .next()
                .context("no tag 'a' found")
                .unwrap();
            let href = elem_a.value().attr("href").unwrap_or("");
            let id = href.trim_start_matches("/message-id/").to_string();
            ans = id == thread.id;
            break;
        }
    }
    ans
}

fn main() -> Result<()> {
    let start_day = "20250101";
    let end_day = "20250201";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day)?;
    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
    Ok(())
}

#[test]
fn test1() {
    let start_day = "20250118";
    let end_day = "20250118";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}

#[test]
fn test2() {
    let start_day = "20250102";
    let end_day = "20250102";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}

#[test]
fn test3() {
    let start_day = "20250106";
    let end_day = "20250106";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}
