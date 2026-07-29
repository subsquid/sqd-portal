use std::{collections::BTreeMap, time::Duration};

use sqd_primitives::BlockNumber;

use crate::{config::Config, metrics::HotblocksRequestOutcome};

/// P-HOTBLOCKS-CONNECT-TIMEOUT.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

pub struct HotblocksHandle {
    pub client: reqwest::Client,
    // Datasets are referenced by their default name in the config.
    // Traceless variants are stored under the key returned by `traceless_key`.
    pub urls: BTreeMap<String, String>,
}

pub fn traceless_key(default_name: &str) -> String {
    format!("{default_name}#traceless")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadMode {
    Finalized,
    RealTime,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub kind: String,
    pub retention_strategy: serde_json::Value,
    pub data: Option<StatusData>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusData {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: String,
    pub last_block_timestamp: Option<u64>,
    pub finalized_head: Option<sqd_primitives::BlockRef>,
}

#[derive(thiserror::Error, Debug)]
pub enum HotblocksErr {
    #[error("Dataset not configured")]
    UnknownDataset,
    #[error("Request to hotblocks database failed: {0}")]
    Request(#[from] reqwest::Error),
}

pub async fn build_client(config: &Config) -> anyhow::Result<HotblocksHandle> {
    tracing::info!("Initializing hotblocks client");

    let mut urls = BTreeMap::new();
    for (default_name, dataset) in config.datasets.iter() {
        if let Some(hotblocks) = &dataset.real_time {
            let remote_name = hotblocks.dataset.as_ref().unwrap_or(default_name);
            let url = hotblocks
                .url
                .join("datasets/")
                .unwrap()
                .join(remote_name)
                .unwrap();
            urls.insert(default_name.clone(), url.into());

            if let Some(traceless_name) = &hotblocks.dataset_traceless {
                let traceless_url = hotblocks
                    .url
                    .join("datasets/")
                    .unwrap()
                    .join(traceless_name)
                    .unwrap();
                urls.insert(traceless_key(default_name), traceless_url.into());
            }
        }
    }

    // Read (idle) timeout, not a total timeout: this client proxies streams, and a
    // total deadline would truncate a stream drained slowly by a backpressured client.
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd()
        // ADR-015 classifies HTTP/1 error shapes; over TLS, ALPN would pick h2 and the
        // replay would stop firing with every test still green.
        .http1_only()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(config.hotblocks_read_timeout);

    if let Some(client_id) = &config.client_id {
        let mut headers = reqwest::header::HeaderMap::new();
        let val = client_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse client id: {}", e))?;
        headers.insert("x-sqd-client-id", val);
        builder = builder.default_headers(headers);
    }

    let client = builder.build()?;

    Ok(HotblocksHandle { client, urls })
}

/// Connection-class per ADR-015: never came up, or died with no response head. A read
/// stall is excluded — it has spent the read budget already, so a replay answers too late.
fn is_connection_class(err: &reqwest::Error) -> bool {
    use std::{error::Error as _, io::ErrorKind};

    // A connect timeout is both, and it is a failed connect. Only a stall reaches below.
    if err.is_connect() {
        return true;
    }
    if err.is_timeout() {
        return false;
    }

    // Walk to the cause, stopping at no error merely recognised: a reset arrives as a bare
    // I/O kind under hyper's predicates, and a wrapping layer can bury it further still.
    let mut source = err.source();
    while let Some(err) = source {
        if let Some(err) = err.downcast_ref::<hyper::Error>() {
            // What the transport hands back for a request it wrote and can't prove unserved.
            if err.is_incomplete_message() || err.is_closed() || err.is_canceled() {
                return true;
            }
        }
        if let Some(err) = err.downcast_ref::<std::io::Error>() {
            if matches!(
                err.kind(),
                ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::NotConnected
            ) {
                return true;
            }
        }
        source = err.source();
    }
    false
}

/// Ensures every started DC-4 request has one final transport outcome even when its future
/// is dropped. The pending cancellation outcome changes when the replay starts, preserving
/// which attempt the caller abandoned without introducing a second metric family.
struct RequestMetricGuard {
    pending: Option<HotblocksRequestOutcome>,
}

impl RequestMetricGuard {
    fn new() -> Self {
        Self {
            pending: Some(HotblocksRequestOutcome::Canceled),
        }
    }

    fn replaying(&mut self) {
        self.pending = Some(HotblocksRequestOutcome::ReplayCanceled);
    }

    fn complete(mut self, outcome: HotblocksRequestOutcome) {
        self.pending = None;
        crate::metrics::report_hotblocks_request(outcome);
    }
}

impl Drop for RequestMetricGuard {
    fn drop(&mut self) {
        if let Some(outcome) = self.pending.take() {
            crate::metrics::report_hotblocks_request(outcome);
        }
    }
}

impl HotblocksHandle {
    /// Sends a DC-4 request, replaying it once on a connection-class fault (ADR-015).
    ///
    /// Wrapping `send` bounds this to faults before the response head — the future resolves
    /// at the head, so a mid-body fault can't reach here (ADR-003, INV-25). A caller that
    /// gave up disconnects, cancelling the replay, so its deadline needs no model (LIV-10).
    async fn send_with_replay(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let replay = request.try_clone();
        let mut request_metric = RequestMetricGuard::new();

        let err = match request.send().await {
            Ok(response) => {
                request_metric.complete(HotblocksRequestOutcome::Response);
                return Ok(response);
            }
            Err(err) => err,
        };

        if !is_connection_class(&err) {
            let outcome = if err.is_timeout() {
                HotblocksRequestOutcome::Timeout
            } else {
                HotblocksRequestOutcome::TransportFailed
            };
            request_metric.complete(outcome);
            return Err(err);
        }
        let Some(replay) = replay else {
            // Unreachable while every DC-4 body is buffered; a streaming one would
            // disable the replay silently.
            tracing::warn!("hotblocks request is not clonable; not replaying");
            request_metric.complete(HotblocksRequestOutcome::TransportFailed);
            return Err(err);
        };

        tracing::info!(error = %err, "hotblocks connection lost before the response head; replaying once");
        request_metric.replaying();
        let result = replay.send().await;
        let outcome = if result.is_ok() {
            HotblocksRequestOutcome::ReplayResponse
        } else {
            HotblocksRequestOutcome::ReplayFailed
        };
        request_metric.complete(outcome);
        result
    }

    pub async fn request_head(
        &self,
        dataset: &str,
        mode: HeadMode,
    ) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let endpoint = match mode {
            HeadMode::Finalized => "finalized-head",
            HeadMode::RealTime => "head",
        };

        let response = self
            .send_with_replay(self.client.get(format!("{url}/{endpoint}")))
            .await?;

        Ok(response)
    }

    /// `None` means the dataset has no matching blocks yet.
    pub async fn get_head(
        &self,
        dataset: &str,
        mode: HeadMode,
    ) -> Result<Option<sqd_primitives::BlockRef>, HotblocksErr> {
        let result = self
            .request_head(dataset, mode)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(result)
    }

    pub async fn request_status(&self, dataset: &str) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let response = self
            .send_with_replay(self.client.get(format!("{url}/status")))
            .await?;

        Ok(response)
    }

    #[tracing::instrument(skip_all, ret, err)]
    pub async fn get_status(&self, dataset: &str) -> Result<Status, HotblocksErr> {
        let result = self
            .request_status(dataset)
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(result)
    }

    pub async fn stream(
        &self,
        dataset: &str,
        query: &str,
        mode: HeadMode,
    ) -> Result<reqwest::Response, HotblocksErr> {
        let Some(url) = self.urls.get(dataset) else {
            return Err(HotblocksErr::UnknownDataset);
        };

        let endpoint = match mode {
            HeadMode::Finalized => "finalized-stream",
            HeadMode::RealTime => "stream",
        };

        let response = self
            .send_with_replay(
                self.client
                    .post(format!("{url}/{endpoint}"))
                    .header("Content-Type", "application/json")
                    .body(query.to_string()),
            )
            .await?;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::*;
    use crate::metrics::{hotblocks_requests, HotblocksRequestOutcome};

    /// Serializes the tests that assert deltas on the process-wide counters.
    static COUNTERS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// What a stub upstream does with one accepted connection.
    #[derive(Clone, Copy)]
    enum Act {
        /// FIN with no response head — a replica terminating mid-request.
        CloseBeforeHead,
        /// RST instead: what a dying peer sends with our request still unread. Carries
        /// none of hyper's predicates, only an I/O kind.
        ResetBeforeHead,
        /// Read the request and say nothing.
        Stall,
        Answer,
        /// Head, then drop mid-body.
        CloseMidBody,
    }

    /// Applies `acts` to successive connections; anything beyond them is closed unanswered.
    async fn stub(acts: Vec<Act>) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));

        let seen = connections.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let act = acts
                    .get(seen.fetch_add(1, Ordering::SeqCst))
                    .copied()
                    .unwrap_or(Act::CloseBeforeHead);
                tokio::spawn(async move {
                    read_request(&mut socket).await;
                    match act {
                        Act::CloseBeforeHead => {}
                        Act::ResetBeforeHead => {
                            let _ = socket.set_linger(Some(Duration::ZERO));
                        }
                        Act::Stall => tokio::time::sleep(Duration::from_secs(30)).await,
                        Act::Answer => {
                            let _ = socket
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                                .await;
                        }
                        Act::CloseMidBody => {
                            let _ = socket
                                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 64\r\n\r\nhalf")
                                .await;
                        }
                    }
                });
            }
        });

        (addr, connections)
    }

    /// Reads the whole head, so the request demonstrably reached the wire — the condition
    /// that makes the transport refuse to replay it itself.
    async fn read_request(socket: &mut TcpStream) {
        let mut seen = Vec::new();
        let mut buf = [0u8; 1024];
        while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
            match socket.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => seen.extend_from_slice(&buf[..n]),
            }
        }
    }

    fn handle(addr: SocketAddr, read_timeout: Duration) -> HotblocksHandle {
        HotblocksHandle {
            client: reqwest::Client::builder()
                .http1_only()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(read_timeout)
                .build()
                .unwrap(),
            urls: BTreeMap::from([("ds".to_owned(), format!("http://{addr}/datasets/ds"))]),
        }
    }

    async fn wait_for_connections(connections: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while connections.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the request did not reach the upstream");
    }

    #[tokio::test]
    async fn replays_once_when_the_peer_closes_before_the_response_head() {
        let _counter = COUNTERS.lock().await;
        let replayed = hotblocks_requests(HotblocksRequestOutcome::ReplayResponse);
        let (addr, connections) = stub(vec![Act::CloseBeforeHead, Act::Answer]).await;
        let handle = handle(addr, Duration::from_secs(5));

        let response = handle.stream("ds", "{}", HeadMode::RealTime).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "one replay, on a new connection"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayResponse) - replayed,
            1,
            "a fault the client never saw must still be counted"
        );
    }

    /// The common shape: a reset carries none of hyper's predicates, so matching only the
    /// clean close would leave most of the incident unreplayed.
    #[tokio::test]
    async fn replays_a_reset_before_the_response_head() {
        let _counter = COUNTERS.lock().await;
        let replayed = hotblocks_requests(HotblocksRequestOutcome::ReplayResponse);
        let (addr, connections) = stub(vec![Act::ResetBeforeHead, Act::Answer]).await;
        let handle = handle(addr, Duration::from_secs(5));

        let response = handle.stream("ds", "{}", HeadMode::RealTime).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(connections.load(Ordering::SeqCst), 2, "one replay");
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayResponse) - replayed,
            1
        );
    }

    #[tokio::test]
    async fn replays_at_most_once() {
        let _counter = COUNTERS.lock().await;
        let replayed = hotblocks_requests(HotblocksRequestOutcome::ReplayResponse);
        let failed = hotblocks_requests(HotblocksRequestOutcome::ReplayFailed);
        let (addr, connections) = stub(vec![Act::CloseBeforeHead, Act::CloseBeforeHead]).await;
        let handle = handle(addr, Duration::from_secs(5));

        handle
            .stream("ds", "{}", HeadMode::RealTime)
            .await
            .unwrap_err();

        assert_eq!(
            connections.load(Ordering::SeqCst),
            2,
            "one replay, never a retry loop (LIV-12)"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayFailed) - failed,
            1,
            "a replay that obtained no head must not land with the ones that did"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayResponse) - replayed,
            0
        );
    }

    #[tokio::test]
    async fn counts_a_replay_canceled_with_its_caller() {
        let _counter = COUNTERS.lock().await;
        let canceled = hotblocks_requests(HotblocksRequestOutcome::ReplayCanceled);
        let failed = hotblocks_requests(HotblocksRequestOutcome::ReplayFailed);
        let (addr, connections) = stub(vec![Act::CloseBeforeHead, Act::Stall]).await;
        let handle = handle(addr, Duration::from_secs(5));

        let request =
            tokio::spawn(async move { handle.stream("ds", "{}", HeadMode::RealTime).await });
        wait_for_connections(&connections, 2).await;

        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayCanceled) - canceled,
            1,
            "a replay dropped with its caller must remain observable"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayFailed) - failed,
            0,
            "caller cancellation is not an observed upstream failure"
        );
    }

    #[tokio::test]
    async fn counts_a_request_canceled_before_replay() {
        let _counter = COUNTERS.lock().await;
        let canceled = hotblocks_requests(HotblocksRequestOutcome::Canceled);
        let replay_canceled = hotblocks_requests(HotblocksRequestOutcome::ReplayCanceled);
        let (addr, connections) = stub(vec![Act::Stall]).await;
        let handle = handle(addr, Duration::from_secs(5));

        let request =
            tokio::spawn(async move { handle.stream("ds", "{}", HeadMode::RealTime).await });
        wait_for_connections(&connections, 1).await;

        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::Canceled) - canceled,
            1,
            "a dropped initial attempt must remain observable"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::ReplayCanceled) - replay_canceled,
            0,
            "the request never reached the replay phase"
        );
    }

    #[tokio::test]
    async fn does_not_replay_after_the_response_head() {
        let _counter = COUNTERS.lock().await;
        let responses = hotblocks_requests(HotblocksRequestOutcome::Response);
        let (addr, connections) = stub(vec![Act::CloseMidBody, Act::Answer]).await;
        let handle = handle(addr, Duration::from_secs(5));

        let response = handle.stream("ds", "{}", HeadMode::RealTime).await.unwrap();

        assert!(response.bytes().await.is_err(), "the body truncates");
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "replaying past the head would duplicate the delivered prefix"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::Response) - responses,
            1,
            "transport accounting ends at the response head"
        );
    }

    #[tokio::test]
    async fn does_not_replay_a_read_stall() {
        let _counter = COUNTERS.lock().await;
        let timed_out = hotblocks_requests(HotblocksRequestOutcome::Timeout);
        let failed = hotblocks_requests(HotblocksRequestOutcome::TransportFailed);
        let (addr, connections) = stub(vec![Act::Stall, Act::Answer]).await;
        let handle = handle(addr, Duration::from_millis(200));

        let err = handle
            .stream("ds", "{}", HeadMode::RealTime)
            .await
            .unwrap_err();

        assert!(matches!(&err, HotblocksErr::Request(e) if e.is_timeout()));
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "a stall has already spent the read budget; replaying it answers too late"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::Timeout) - timed_out,
            1,
            "the non-replayed timeout must remain visible in the unified metric"
        );
        assert_eq!(
            hotblocks_requests(HotblocksRequestOutcome::TransportFailed) - failed,
            0,
            "a read stall has its own outcome"
        );
    }
}
