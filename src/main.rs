use anyhow::{Context, Ok, Result};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};
use const_format::concatcp;
use phf::phf_map;
use reqwest::blocking::Client;
use scraper::{Html, Selector};

const PG_SITE: &str = "https://www.postgresql.org";
const WHOLE_THREAD_URL_PREFIX: &str = concatcp!(PG_SITE, "/message-id/flat");
const NEXT_THREADS_URL_PREFIX: &str = concatcp!(PG_SITE, "/list/pgsql-hackers/since");

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

fn clean_subject_title(title: &str) -> String {
    let title = title.trim();
    // remove unicode emoji
    let title = title.split('📎').next().unwrap_or(title).trim().to_string();
    // replace multiple spaces with single one
    let mut new_title = String::new();
    let mut prev_char = ' ';
    for char in title.chars() {
        if char.is_whitespace() && !prev_char.is_whitespace() {
            new_title.push(' ');
        } else if !char.is_whitespace() {
            new_title.push(char);
        }
        prev_char = char;
    }
    new_title
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
        if tds.is_empty() {
            continue;
        }

        if let (Some(subject_td), true) = (subject_th, tds.len() >= 2) {
            let author_td = &tds[0];
            let time_td = &tds[1];

            // Get subject and URL
            if let Some(a) = subject_td.select(&a_selector).next() {
                let text = a.text().collect::<String>().trim().to_string();
                let clean_subject = clean_subject_title(&text);

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

/// handle threads of each day found in the page.
/// when `handle` returns `false`, the processing is stopped.
fn for_each_thread(url: &str, mut handle: impl FnMut(EmailThread) -> bool) -> Result<()> {
    let document = get_document(url)?;

    // Find all elements
    let h2_selector = Selector::parse("h2").unwrap();
    // Next to h2, find table
    let table_selector = Selector::parse("h2 + table").unwrap();
    let mut table_iter = document.select(&table_selector);

    // First find the date
    for h2 in document.select(&h2_selector) {
        let date_text = h2.text().collect::<String>();
        if let Some(date) = transform_date(&date_text) {
            if let Some(false) = table_iter
                .next()
                .map(|table| handle_table(&table, date, &mut handle))
            {
                break;
            }
        }
    }
    Ok(())
}

// Get new subjects between start_day and end_day (inclusive)
fn get_new_subjects_between(start_day: &str, end_day: &str) -> Result<Vec<EmailThread>> {
    let mut start_date: NaiveDateTime = NaiveDate::parse_from_str(start_day, "%Y%m%d")
        .context("parse start date")?
        .into();
    let end_date: NaiveDateTime = NaiveDate::parse_from_str(end_day, "%Y%m%d")
        .context("parse end date")?
        .and_hms_opt(23, 59, 59)
        .unwrap();
    let mut threads: Vec<EmailThread> = Vec::new();

    // we use following two variables to ensure we process each date fully and exactly once
    let mut current_size = 0;
    let mut prev_date = start_date
        .checked_sub_signed(TimeDelta::seconds(1))
        .unwrap();

    // process all threads between, like 20250101-00:00:00 and 20250101-23:59:59
    while start_date <= end_date {
        println!("start_date={start_date:#?} end_date={end_date:#?}");

        // if the start_date was processed already, we are done with all dates
        if prev_date == start_date {
            break;
        }
        prev_date = start_date;

        let current_url = format!(
            "{NEXT_THREADS_URL_PREFIX}/{}",
            start_date.format("%Y%m%d%H%M")
        );

        // It is possbile that we get part of data in the last day in the current page and get the same
        // part of data in the next page of the same day. For example, we get some threads published parallelly
        // at 20250212-13:58, and get next page from '/list/pgsql-hackers/since/202502121358', then we will get
        // the same threads again of time 20250212-13:58. We need to remove the duplicates.
        let mut has_dups = true;
        for_each_thread(&current_url, |thread| {
            if has_dups {
                for thr in threads.iter().rev() {
                    if thr.id == thread.id {
                        has_dups = true;
                        return true; // return early for next thread
                    }
                }
                has_dups = false;
            }

            let whole_thread_url = format!("{WHOLE_THREAD_URL_PREFIX}/{}", thread.id);
            start_date = thread.datetime;

            // we only handle threads between start_date and end_date
            let in_range = start_date <= end_date;
            if in_range && is_first_email(&whole_thread_url, &thread) {
                threads.push(thread);
            }
            in_range
        })
        .context("Failed to process email threads")?;

        // not get any new thread
        if current_size == threads.len() {
            break;
        }
        current_size += threads.len();
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

    if thread.subject.starts_with("Re：")
        || thread.subject.starts_with("re：")
        || thread.subject.starts_with("RE：")
        || thread.subject.starts_with("rE：")
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
            let elem_td = elem_tr.select(&tag_td).next().unwrap();
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
    // has Chinese ':' in the subject title, like this: 'Re：Limit length of queryies in pg_stat_statement extension'
    let start_day = "20250118";
    let end_day = "20250118";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    assert!(thread_emails.len() == 1);

    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}

#[test]
fn test2() {
    // has Re: in subject title, like this: 'Fwd: Re: A new look at old NFS readdir() problems?'
    let start_day = "20250102";
    let end_day = "20250102";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    assert!(thread_emails
        .iter()
        .any(|thread| thread.subject.contains("Re:")));

    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}

#[test]
fn test3() {
    // has unicode emoji and '\n' in the subject title
    let start_day = "20250106";
    let end_day = "20250106";
    println!("Fetching emails from: {} ~ {}", start_day, end_day);
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();
    assert!(thread_emails
        .iter()
        .any(|thread| !thread.subject.contains('\n')));

    println!("\nFirst emails in each thread:");
    println!("----------------------------");
    for thread in thread_emails {
        println!("{}", thread);
        println!();
    }
}

#[test]
fn test4() {
    let start_day = "20240104";
    let end_day = "20240104";
    let thread_emails_20240104 = get_new_subjects_between(start_day, end_day).unwrap();
    let start_day = "20240105";
    let end_day = "20240105";
    let thread_emails_20240105 = get_new_subjects_between(start_day, end_day).unwrap();
    let start_day = "20240106";
    let end_day = "20240106";
    let thread_emails_20240106 = get_new_subjects_between(start_day, end_day).unwrap();

    let start_day = "20240104";
    let end_day = "20240106";
    let thread_emails = get_new_subjects_between(start_day, end_day).unwrap();

    assert!(
        thread_emails_20240104.len() + thread_emails_20240105.len() + thread_emails_20240106.len()
            == thread_emails.len()
    );
    assert!(thread_emails.iter().all(|thread| {
        thread_emails_20240104.iter().any(|t| t.id == thread.id)
            || thread_emails_20240105.iter().any(|t| t.id == thread.id)
            || thread_emails_20240106.iter().any(|t| t.id == thread.id)
    }));
}
