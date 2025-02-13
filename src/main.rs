use anyhow::{Context, Ok, Result};
use chrono::{NaiveDate, NaiveDateTime, TimeDelta};
use const_format::concatcp;
use phf::phf_map;
use reqwest::blocking::Client;
use scraper::{Html, Selector};

const PG_SITE: &str = "https://www.postgresql.org";
const MESSAGE_URL_PREFIX: &str = concatcp!(PG_SITE, "/message-id");
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

trait PgMessage {
    fn id(&self) -> &str;
}

#[derive(Debug)]
struct EmailThread {
    id: String,
    subject: String,
    datetime: NaiveDateTime,
    author: String,
}

impl PgMessage for EmailThread {
    fn id(&self) -> &str {
        &self.id
    }
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

#[derive(Debug)]
struct EmailThreadDetail {
    id: String,
    subject: String,
    datetime: NaiveDateTime,
    author_name: String,
    author_email: String,
    content: String,
    // name and url
    attachments: Vec<(String, String)>,
    // list of other messages' id
    replies: Vec<String>,
}

impl PgMessage for EmailThreadDetail {
    fn id(&self) -> &str {
        &self.id
    }
}

impl std::fmt::Display for EmailThreadDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Thread: {}\n\
            Author Name: {}\n\
            Author Email: {}\n\
            Time: {}\n\
            URL: {PG_SITE}/message-id/{}\n\
            Content Size: {}\n\
            Total Attachments: {}\n\
            Total replies: {}",
            self.subject,
            self.author_name,
            self.author_email,
            self.datetime.format("%Y-%m-%d %H:%M:%S"),
            self.id,
            self.content.len(),
            self.attachments.len(),
            self.replies.len(),
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
    let start_time = std::time::Instant::now();
    let response = client.get(url).send().context("Failed to fetch the page")?;
    let body = response.text().context("Failed to get response text")?;
    println!("get document from {url}, done, elapsed: {} ms", start_time.elapsed().as_millis());

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

fn get_threads_between<T: PgMessage>(
    start_day: &str,
    end_day: &str,
    mut handle: impl FnMut(EmailThread) -> Option<T>,
) -> Result<Vec<T>> {
    let mut start_date: NaiveDateTime = NaiveDate::parse_from_str(start_day, "%Y%m%d")
        .context("parse start date")?
        .into();
    let end_date: NaiveDateTime = NaiveDate::parse_from_str(end_day, "%Y%m%d")
        .context("parse end date")?
        .and_hms_opt(23, 59, 59)
        .unwrap();
    let mut threads: Vec<T> = Vec::new();

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
                    if thr.id() == thread.id {
                        has_dups = true;
                        return true; // return early for next thread
                    }
                }
                has_dups = false;
            }

            start_date = thread.datetime;

            // we only handle threads between start_date and end_date
            let in_range = start_date <= end_date;
            if in_range {
                if let Some(thread) = handle(thread) {
                    threads.push(thread);
                }
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

// Get new subjects between start_day and end_day (inclusive)
fn get_new_subjects_between(start_day: &str, end_day: &str) -> Result<Vec<EmailThread>> {
    get_threads_between(start_day, end_day, |thread| {
        if is_thread_starter(&thread) {
            Some(thread)
        } else {
            None
        }
    })
}

/// active subject is the subject under discussion, including reply thread and new thread
fn get_active_subjects_between(start_day: &str, end_day: &str) -> Result<Vec<EmailThreadDetail>> {
    let mut seen_ids = std::collections::HashSet::new();
    get_threads_between(start_day, end_day, |thread| {
        let id = get_thread_starter_id(&thread.id);
        if seen_ids.contains(&id) {
            None
        } else {
            let t = get_thread_by_id(&id);
            seen_ids.insert(id);
            Some(t)
        }
    })
}

fn get_thread_by_id(id: &str) -> EmailThreadDetail {
    let message_url = format!("{MESSAGE_URL_PREFIX}/{id}");
    let doc = get_document(&message_url)
        .context("failed to get the email")
        .unwrap();

    let table_tag_name = "#pgContentWrap table";
    let table_tag = Selector::parse(table_tag_name).unwrap();
    let select_tag = Selector::parse("select#thread_select").unwrap();
    let option_tag = Selector::parse("option").unwrap();
    let tr_tag = Selector::parse("tr").unwrap();
    let td_tag = Selector::parse("td").unwrap();
    let content_tag_name = "#pgContentWrap div.message-content";
    let content_tag = Selector::parse(content_tag_name).unwrap();
    let attchm_tag_name = "#pgContentWrap table.message-attachments";
    let attchm_tag = Selector::parse(attchm_tag_name).unwrap();
    let th_tag = Selector::parse("th").unwrap();
    let a_tag = Selector::parse("a").unwrap();

    let tr_elems: Vec<_> = doc
        .select(&table_tag)
        .next()
        .context(format!("no tag '{table_tag_name}' found in the page"))
        .unwrap()
        .select(&tr_tag)
        .collect();

    let replies: Vec<_> = doc
        .select(&select_tag)
        .next()
        .context("no 'select' tag in the page")
        .unwrap()
        .select(&option_tag)
        .map(|opt_elem| opt_elem.value().attr("value").unwrap_or("").to_string())
        .collect();

    let content_elem = doc
        .select(&content_tag)
        .next()
        .context(format!("no tag '{content_tag_name}' found"))
        .unwrap();
    let content = content_elem.text().collect::<String>().trim().to_string();

    let mut attachments = Vec::new();
    if let Some(attchm_elem) = doc.select(&attchm_tag).next() {
        for att in attchm_elem.select(&th_tag) {
            if let Some(link) = att.select(&a_tag).next() {
                attachments.push((
                    link.value().attr("href").unwrap_or("").to_string(),
                    link.text().collect::<String>().trim().to_string(),
                ));
            }
        }
    }

    let (from_elem, subject_elem, datetime_elem) = if tr_elems.len() == 8 {
        (tr_elems[0], tr_elems[2], tr_elems[3])
    } else if tr_elems.len() == 9 {
        (tr_elems[0], tr_elems[3], tr_elems[4])
    } else {
        panic!("the table has neither 8 or 9 rows");
    };
    let td_elem = from_elem.select(&td_tag).next().unwrap();
    let author_details = td_elem.text().collect::<String>().trim().to_string();
    let mut author_details = author_details.split('<');
    let author_name = author_details.next().unwrap_or("").trim().to_string();
    let author_email = author_details
        .next()
        .unwrap_or("")
        .trim_end_matches(">")
        .replace("(dot)", ".")
        .replace("(at)", "@");

    let td_elem = subject_elem.select(&td_tag).next().unwrap();
    let subject = td_elem.text().collect::<String>().trim().to_string();

    let td_elem = datetime_elem.select(&td_tag).next().unwrap();
    let datetime_str = td_elem.text().collect::<String>().trim().to_string();
    let datetime = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S")
        .context("invalid datetime format")
        .unwrap();

    EmailThreadDetail {
        id: id.to_string(),
        subject,
        datetime,
        author_name,
        author_email,
        content,
        attachments,
        replies,
    }
}

fn is_thread_starter(thread: &EmailThread) -> bool {
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

    is_thread_starter_by_id(&thread.id)
}

#[allow(unused)]
fn get_subject_thread_id_list(id: &str) -> Result<Vec<String>> {
    let message_url = format!("{MESSAGE_URL_PREFIX}/{id}");
    let select_tag = Selector::parse("select#thread_select").unwrap();
    let option_tag = Selector::parse("option").unwrap();

    get_document(&message_url)
        .context("failed to get document")
        .unwrap()
        .select(&select_tag)
        .next()
        .context("no 'select' tag in the page")
        .and_then(|select| {
            Ok(select
                .select(&option_tag)
                .map(|opt_elem| opt_elem.value().attr("value").unwrap_or("").to_string())
                .collect::<Vec<_>>())
        })
}

fn get_thread_starter_id(id: &str) -> String {
    let message_url = format!("{MESSAGE_URL_PREFIX}/{id}");
    let select_tag = Selector::parse("select#thread_select").unwrap();
    let option_tag = Selector::parse("option").unwrap();

    get_document(&message_url)
        .context("failed to get document")
        .unwrap()
        .select(&select_tag)
        .next()
        .context("no 'select' tag in the page")
        .unwrap()
        .select(&option_tag)
        .next()
        .context("no 'option' tag in 'select' tag")
        .unwrap()
        .value()
        .attr("value")
        .map(|value| value.to_string())
        .context("no 'value' tag in the 'option' tag")
        .unwrap()
}

fn is_thread_starter_by_id(id: &str) -> bool {
    get_thread_starter_id(id) == id
}

fn main() -> Result<()> {
    use chrono::Local;

    let args: Vec<_> = std::env::args().collect();
    let get_active = args.len() == 2 && args[1] == "active";

    if get_active {
        let current_datetime = Local::now().naive_local();
        let end_day = current_datetime.format("%Y%m%d").to_string();
        let start_day = (current_datetime - TimeDelta::days(1))
            .format("%Y%m%d")
            .to_string();

        println!("Fetching all subjects under discussion for {start_day} ~ {end_day}");
        let thread_emails = get_active_subjects_between(&start_day, &end_day)?;
        println!("----------------------------");
        for thread in thread_emails {
            println!("{}", thread);
            println!();
        }
    } else {
        let current_datetime = Local::now().naive_local();
        let end_day = current_datetime.format("%Y%m%d").to_string();
        let start_day = (current_datetime - TimeDelta::days(7))
            .format("%Y%m%d")
            .to_string();

        println!(
            "Fetching new topics for last week from: {} ~ {}",
            start_day, end_day
        );
        let thread_emails = get_new_subjects_between(&start_day, &end_day)?;
        println!("----------------------------");
        for thread in thread_emails {
            println!("{}", thread);
            println!();
        }
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
