//! Serialize adapter ownership without holding admission ahead of collection.
use super::{progress, wal_error};
use crate::{AppendCompletion, WalHandle, WalSeqNo};
use bytes::Bytes;
use futures::future::BoxFuture;
use slatedb::wal::WalError;
use tokio::sync::{mpsc, oneshot, watch};

struct Append {
    index: WalSeqNo,
    bytes: Bytes,
    position: progress::Position,
    reply: oneshot::Sender<Result<Admission, WalError>>,
}

// If admission succeeded but the caller cancelled before consuming its reply,
// fail closed rather than allowing it to reuse an already admitted record ID.
struct Admission(Option<watch::Sender<progress::Progress>>);
impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(updates) = self.0.take() {
            updates.send_modify(|progress| progress.fail(WalError::Closed));
        }
    }
}

pub(super) struct Collect {
    pub floor: WalSeqNo,
    pub reply: oneshot::Sender<Result<(), WalError>>,
}

pub(super) struct Handle {
    append: mpsc::Sender<Append>,
    pub collect: mpsc::Sender<Collect>,
    pub progress: watch::Sender<progress::Progress>,
    stop: oneshot::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), WalError>>,
}

struct Engine(Option<WalHandle>);
impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            tokio::runtime::Handle::current().spawn(handle.abort());
        }
    }
}

impl Handle {
    pub fn start(handle: WalHandle, recovered_end: u64) -> Self {
        let (append, mut appends) = mpsc::channel::<Append>(1);
        let (collect, mut collections) = mpsc::channel::<Collect>(16);
        let (stop, mut stopping) = oneshot::channel();
        let (progress, _) = watch::channel(progress::Progress::default());
        let updates = progress.clone();
        let task = tokio::spawn(async move {
            let mut engine = Engine(Some(handle));
            let (tickets, mut completions) =
                mpsc::unbounded_channel::<(AppendCompletion, progress::Position)>();
            // One task drains ordered public completions. It never runs listeners.
            let completion_updates = updates.clone();
            let drainer = tokio::spawn(async move {
                while let Some((ticket, position)) = completions.recv().await {
                    progress::completion(completion_updates.clone(), position)(ticket.await);
                }
            });
            let mut pending: Option<Append> = None;
            // At most one collection is active; the bounded channel holds the
            // rest. The owned maintenance future does not borrow admission.
            let mut collection: Option<BoxFuture<'static, ()>> = None;
            loop {
                let index = pending.as_ref().map(|request| request.index);
                let bytes = pending.as_ref().map(|request| request.bytes.clone());
                enum Event {
                    Stop(bool),
                    Append(Option<Append>),
                    Collect(Collect),
                    Collected,
                    Cancel,
                    Admitted(Result<AppendCompletion, crate::Error>),
                }
                let event = tokio::select! {
                    biased;
                    stop = &mut stopping => Event::Stop(stop.unwrap_or(true)),
                    () = async { collection.as_mut().unwrap().await }, if collection.is_some() => Event::Collected,
                    Some(request) = collections.recv(), if collection.is_none() => Event::Collect(request),
                    () = async { pending.as_mut().unwrap().reply.closed().await }, if index.is_some() => Event::Cancel,
                    result = async { engine.0.as_mut().unwrap().enqueue_append(index.unwrap(), bytes.unwrap()).await }, if index.is_some() => Event::Admitted(result),
                    request = appends.recv(), if index.is_none() => Event::Append(request),
                };
                // Cancellation during admission consumes no record ID.
                match event {
                    Event::Stop(abort) => {
                        let handle = engine.0.take().unwrap();
                        let result = if abort {
                            handle.abort().await;
                            Err(WalError::Closed)
                        } else {
                            handle.shutdown().await.map_err(wal_error)
                        };
                        drop(pending);
                        drop(tickets);
                        drainer.await.map_err(|_| WalError::Closed)?;
                        return result;
                    }
                    Event::Append(Some(request)) => {
                        if let Some(error) = updates.borrow().failure.clone() {
                            let _ = request.reply.send(Err(error));
                        } else {
                            pending = Some(request);
                        }
                    }
                    Event::Append(None) => {
                        engine.0.take().unwrap().abort().await;
                        drop(tickets);
                        let _ = drainer.await;
                        return Err(WalError::Closed);
                    }
                    Event::Collect(request) => {
                        let committed_end = updates
                            .borrow()
                            .committed
                            .map_or(recovered_end, |position| position.wal_id.max(recovered_end));
                        let floor = WalSeqNo::record(request.floor.record_index.min(committed_end));
                        let truncation = engine.0.as_ref().unwrap().truncate_before(floor);
                        collection = Some(Box::pin(async move {
                            let result = truncation.await.map(|_| ()).map_err(wal_error);
                            let _ = request.reply.send(result);
                        }));
                    }
                    Event::Collected => collection = None,
                    Event::Cancel => pending = None,
                    Event::Admitted(result) => {
                        let request = pending.take().unwrap();
                        let result = result
                            .map_err(wal_error)
                            .and_then(|ticket| {
                                tickets
                                    .send((ticket, request.position))
                                    .map_err(|_| WalError::Closed)
                            })
                            .map(|()| Admission(Some(updates.clone())));
                        let _ = request.reply.send(result);
                    }
                }
            }
        });
        Self {
            append,
            collect,
            progress,
            stop,
            task,
        }
    }

    pub async fn append(
        &self,
        index: WalSeqNo,
        bytes: Bytes,
        position: progress::Position,
    ) -> Result<(), WalError> {
        let (reply, response) = oneshot::channel();
        self.append
            .send(Append {
                index,
                bytes,
                position,
                reply,
            })
            .await
            .map_err(|_| WalError::Closed)?;
        let mut admission = response.await.map_err(|_| WalError::Closed)??;
        admission.0 = None;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), WalError> {
        let _ = self.stop.send(false);
        self.task.await.map_err(|_| WalError::Closed)?
    }

    pub async fn abort(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}
