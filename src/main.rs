mod error;
mod queue;
use error::{Error, Result};
use queue::AsyncQueue;

use actix_web::{
    get,
    middleware::Logger,
    put,
    web::{self, Bytes},
    App, HttpResponse, HttpServer, Result as WebResult,
};
use anyhow::Result as AnyResult;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use crate::queue::ValueOrReciever;

#[derive(Clone, Default)]
struct QueueMap<K, V> {
    queues: Arc<Mutex<HashMap<K, AsyncQueue<V>>>>,
}

impl<K, V> QueueMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    async fn push(&self, key: K, value: V, capacity: Option<usize>) -> Result<()> {
        let mut queues = self.queues.lock().await;
        queues
            .entry(key)
            .and_modify(|queue| {
                if queue.len() + queue.subscribers() == 0 {
                    *queue = AsyncQueue::new(capacity)
                }
            })
            .or_insert_with(|| AsyncQueue::new(capacity))
            .push(value)?;
        Ok(())
    }

    async fn pop(&self, key: &K) -> Result<V> {
        let mut queues = self.queues.lock().await;
        queues.get_mut(key).ok_or(Error::QueueNotFound)?.pop()
    }

    async fn pop_with_timeout(&self, key: K, timeout: Duration) -> Result<V> {
        let result = {
            let mut queues = self.queues.lock().await;
            let queue = queues.entry(key).or_insert_with(|| AsyncQueue::new(None));
            queue.pop_with_subscribe()
        };
        match result {
            ValueOrReciever::Value(value) => Ok(value),
            ValueOrReciever::Receiver(mut receiver) => {
                tokio::time::timeout(timeout, receiver.recv())
                    .await
                    .map_err(|_| Error::GetTimeout)?
                    .ok_or(Error::Internal)
            }
        }
    }
}

#[derive(Default, Clone)]
struct AppState {
    queue_map: QueueMap<String, Bytes>,
}

#[derive(Deserialize)]
struct PopQueryParams {
    #[serde(default)]
    #[serde(with = "humantime_serde")]
    timeout: Option<Duration>,
}

#[get("/{queue_name}")]
async fn pop_from_queue(
    state: web::Data<AppState>,
    web::Path(queue_name): web::Path<String>,
    query: web::Query<PopQueryParams>,
) -> WebResult<Bytes> {
    if let Some(timeout) = query.timeout {
        Ok(state
            .queue_map
            .pop_with_timeout(queue_name, timeout)
            .await?)
    } else {
        Ok(state.queue_map.pop(&queue_name).await?)
    }
}

#[put("/{queue_name}")]
async fn put_to_queue(
    state: web::Data<AppState>,
    value: Bytes,
    web::Path(queue_name): web::Path<String>,
) -> WebResult<HttpResponse> {
    state.queue_map.push(queue_name, value, None).await?;
    Ok(HttpResponse::Ok().finish())
}

#[put("/{queue_name}/{capacity}")]
async fn put_to_queue_with_capacity(
    state: web::Data<AppState>,
    value: Bytes,
    web::Path((queue_name, capacity)): web::Path<(String, usize)>,
) -> WebResult<HttpResponse> {
    state
        .queue_map
        .push(queue_name, value, Some(capacity))
        .await?;
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::main]
async fn main() -> AnyResult<()> {
    init_logger();
    let state = AppState::default();
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .data(state.clone())
            .service(put_to_queue)
            .service(put_to_queue_with_capacity)
            .service(pop_from_queue)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await?;
    Ok(())
}

fn init_logger() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
