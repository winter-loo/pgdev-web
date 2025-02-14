use axum::{
    routing::get,
    Router,
    Json,
    extract::Query,
};
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use tower_http::cors::{CorsLayer, Any};

use crate::{get_active_subjects_between, get_new_subjects_between, EmailThread, EmailThreadDetail};

#[derive(Debug, Deserialize)]
pub struct DateRangeQuery {
    start_date: String,
    end_date: String,
}

#[derive(Debug, Serialize)]
pub struct EmailThreadResponse {
    id: String,
    subject: String,
    datetime: NaiveDateTime,
    author: String,
}

#[derive(Debug, Serialize)]
pub struct EmailThreadDetailResponse {
    id: String,
    subject: String,
    datetime: NaiveDateTime,
    author_name: String,
    author_email: String,
    content: String,
}

impl From<EmailThread> for EmailThreadResponse {
    fn from(thread: EmailThread) -> Self {
        Self {
            id: thread.id,
            subject: thread.subject,
            datetime: thread.datetime,
            author: thread.author,
        }
    }
}

impl From<EmailThreadDetail> for EmailThreadDetailResponse {
    fn from(detail: EmailThreadDetail) -> Self {
        Self {
            id: detail.id,
            subject: detail.subject,
            datetime: detail.datetime,
            author_name: detail.author_name,
            author_email: detail.author_email,
            content: detail.content,
        }
    }
}

async fn get_active_subjects(
    Query(params): Query<DateRangeQuery>,
) -> Json<Vec<EmailThreadDetailResponse>> {
    let start_date = NaiveDateTime::parse_from_str(&params.start_date, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::from_timestamp_opt(0, 0).unwrap());
    let end_date = NaiveDateTime::parse_from_str(&params.end_date, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| chrono::Local::now().naive_local());

    let subjects = get_active_subjects_between(start_date, end_date)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(EmailThreadDetailResponse::from)
        .collect();

    Json(subjects)
}

async fn get_new_subjects(
    Query(params): Query<DateRangeQuery>,
) -> Json<Vec<EmailThreadResponse>> {
    let start_date = NaiveDateTime::parse_from_str(&params.start_date, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| NaiveDateTime::from_timestamp_opt(0, 0).unwrap());
    let end_date = NaiveDateTime::parse_from_str(&params.end_date, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| chrono::Local::now().naive_local());

    let subjects = get_new_subjects_between(start_date, end_date)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(EmailThreadResponse::from)
        .collect();

    Json(subjects)
}

pub fn create_router() -> Router {
    Router::new()
        .route("/api/active-subjects", get(get_active_subjects))
        .route("/api/new-subjects", get(get_new_subjects))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
}
