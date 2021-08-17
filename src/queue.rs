use crate::error::{Error, Result};

use std::collections::VecDeque;
use tokio::sync::mpsc::{self, error::TrySendError, Receiver, Sender};

pub struct AsyncQueue<T> {
    inner: VecDeque<T>,
    subscribers: VecDeque<Sender<T>>,
    capacity: Option<usize>,
}

pub enum ValueOrReciever<T> {
    Value(T),
    Receiver(Receiver<T>),
}

impl<T> AsyncQueue<T> {
    pub fn new(capacity: Option<usize>) -> Self {
        Self {
            capacity,
            inner: Default::default(),
            subscribers: Default::default(),
        }
    }

    pub fn push(&mut self, value: T) -> Result<()> {
        if let Some(value) = self.try_send(value) {
            match self.capacity {
                Some(capacity) if self.inner.len() >= capacity => return Err(Error::QueueIsFull),
                _ => {}
            }
            self.inner.push_back(value);
        }
        Ok(())
    }

    fn try_send(&mut self, mut value: T) -> Option<T> {
        while let Some(mut sender) = self.subscribers.pop_front() {
            value = match sender.try_send(value) {
                Ok(_) => return None,
                Err(TrySendError::Closed(value)) | Err(TrySendError::Full(value)) => value,
            }
        }
        Some(value)
    }

    pub fn pop(&mut self) -> Result<T> {
        self.inner.pop_front().ok_or(Error::QueueIsEmpty)
    }

    pub fn pop_with_subscribe(&mut self) -> ValueOrReciever<T> {
        let (sender, receiver) = mpsc::channel(1);
        if let Some(value) = self.inner.pop_front() {
            ValueOrReciever::Value(value)
        } else {
            self.subscribers.push_back(sender);
            ValueOrReciever::Receiver(receiver)
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn subscribers(&self) -> usize {
        self.subscribers.len()
    }
}
