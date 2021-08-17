use actix_web::{
    error,
    http::{header, StatusCode},
    HttpResponse,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("queue not found")]
    QueueNotFound,

    #[error("queue is full")]
    QueueIsFull,

    #[error("queue is empty")]
    QueueIsEmpty,

    #[error("timeout")]
    GetTimeout,

    #[error("internal")]
    Internal,
}

impl error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code())
            .set_header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(self.to_string())
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::QueueNotFound => StatusCode::NOT_FOUND,
            Error::QueueIsEmpty => StatusCode::NOT_FOUND,
            Error::QueueIsFull => StatusCode::TOO_MANY_REQUESTS,
            Error::GetTimeout => StatusCode::NOT_FOUND,
            Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub type Result<T, E = Error> = core::result::Result<T, E>;
