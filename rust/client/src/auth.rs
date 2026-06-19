use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use google_cloud_auth::credentials::{AccessTokenCredentials, Builder as AdcCredentialsBuilder};
use tonic::metadata::MetadataValue;

use crate::error::Error;

const GCS_READ_WRITE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";
const TOKEN_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
/// Controls how aggressively a refreshing credential is renewed.
///
/// The source is queried once during construction, then every
/// [`refresh_interval`](Self::refresh_interval). A failed refresh keeps the
/// last valid token and retries after [`retry_interval`](Self::retry_interval).
pub struct RefreshingAuthConfig {
    /// Delay between successful token refreshes. Choose a value comfortably
    /// below the credential lifetime; the default is appropriate for ADC.
    pub refresh_interval: Duration,
    /// Backoff after a token-source failure. Existing clients continue using
    /// the last valid token during this interval.
    pub retry_interval: Duration,
}

impl Default for RefreshingAuthConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
            retry_interval: Duration::from_secs(5),
        }
    }
}

#[async_trait]
/// Supplies bearer tokens to [`BearerAuth`].
///
/// Implement this for workload identity systems other than Google ADC. Return
/// the raw token without a `Bearer ` prefix.
pub trait AccessTokenSource: Send + Sync {
    /// Fetch a currently usable access token.
    async fn access_token(&self) -> Result<String, Error>;
}

#[derive(Clone)]
/// Cloneable authentication handle shared by existing gRPC clients.
///
/// Refreshing instances publish new tokens through `ArcSwap`, so clones and
/// already-created [`crate::GrpcReplicaFactory`] values observe updates without
/// reconnecting. Dropping the last clone stops the refresh task.
pub struct BearerAuth {
    token: Arc<ArcSwap<String>>,
    refresh_task: Option<Arc<RefreshTask>>,
}

impl fmt::Debug for BearerAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerAuth")
            .field("refreshing", &self.refresh_task.is_some())
            .finish_non_exhaustive()
    }
}

impl BearerAuth {
    /// Create a non-refreshing token handle for tests or controlled deployments.
    ///
    /// Prefer [`google_adc`](Self::google_adc) in production.
    pub fn static_token(token: impl Into<String>) -> Self {
        Self {
            token: Arc::new(ArcSwap::from_pointee(token.into())),
            refresh_task: None,
        }
    }

    /// Create a refreshing handle backed by a custom token source.
    ///
    /// Construction fetches and validates the initial token before returning;
    /// callers never receive a handle without a usable credential.
    pub async fn refreshing(
        source: Arc<dyn AccessTokenSource>,
        config: RefreshingAuthConfig,
    ) -> Result<Self, Error> {
        if config.refresh_interval.is_zero() || config.retry_interval.is_zero() {
            return Err(Error::InvalidRefreshInterval);
        }
        let initial = source.access_token().await?;
        validate_token(&initial)?;
        let token = Arc::new(ArcSwap::from_pointee(initial));
        let task_token = Arc::clone(&token);
        let task = tokio::spawn(async move {
            let mut delay = config.refresh_interval;
            loop {
                tokio::time::sleep(delay).await;
                match fetch_refresh_token(source.as_ref(), TOKEN_REFRESH_TIMEOUT).await {
                    Some(Ok(next)) => match validate_token(&next) {
                        Ok(()) => {
                            task_token.store(Arc::new(next));
                            delay = config.refresh_interval;
                        }
                        Err(error) => {
                            tracing::warn!(%error, "auth token refresh returned an invalid token");
                            delay = config.retry_interval;
                        }
                    },
                    Some(Err(error)) => {
                        tracing::warn!(%error, "auth token refresh failed");
                        delay = config.retry_interval;
                    }
                    None => {
                        // google-cloud-auth's remote ADC sources use reqwest,
                        // whose client has no total request timeout by default.
                        // Bound only renewal: the last validated token remains
                        // available while the source is retried.
                        tracing::warn!(
                            timeout_seconds = TOKEN_REFRESH_TIMEOUT.as_secs(),
                            "auth token refresh timed out"
                        );
                        delay = config.retry_interval;
                    }
                }
            }
        });
        Ok(Self {
            token,
            refresh_task: Some(Arc::new(RefreshTask(task))),
        })
    }

    /// Create the production Google Application Default Credentials path.
    ///
    /// The credential requests the GCS read/write scope and refreshes in the
    /// background according to `config`.
    pub async fn google_adc(config: RefreshingAuthConfig) -> Result<Self, Error> {
        let credentials = AdcCredentialsBuilder::default()
            .with_scopes([GCS_READ_WRITE_SCOPE])
            .build_access_token_credentials()
            .map_err(|error| Error::TokenSource(error.to_string()))?;
        Self::refreshing(Arc::new(GoogleAdcTokenSource(credentials)), config).await
    }

    pub(crate) fn authorization_header(
        &self,
    ) -> Result<MetadataValue<tonic::metadata::Ascii>, Error> {
        let token = self.token.load();
        MetadataValue::try_from(format!("Bearer {}", token.as_str()))
            .map_err(|_| Error::InvalidToken)
    }
}

struct RefreshTask(tokio::task::JoinHandle<()>);

impl Drop for RefreshTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug)]
struct GoogleAdcTokenSource(AccessTokenCredentials);

#[async_trait]
impl AccessTokenSource for GoogleAdcTokenSource {
    async fn access_token(&self) -> Result<String, Error> {
        self.0
            .access_token()
            .await
            .map(|token| token.token)
            .map_err(|error| Error::TokenSource(error.to_string()))
    }
}

async fn fetch_refresh_token(
    source: &dyn AccessTokenSource,
    timeout: Duration,
) -> Option<Result<String, Error>> {
    tokio::time::timeout(timeout, source.access_token())
        .await
        .ok()
}

fn validate_token(token: &str) -> Result<(), Error> {
    if token.is_empty() {
        return Err(Error::InvalidToken);
    }
    MetadataValue::try_from(format!("Bearer {token}"))
        .map(|_| ())
        .map_err(|_| Error::InvalidToken)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Debug)]
    struct CountingSource(AtomicUsize);

    #[async_trait]
    impl AccessTokenSource for CountingSource {
        async fn access_token(&self) -> Result<String, Error> {
            Ok(format!("token-{}", self.0.fetch_add(1, Ordering::SeqCst)))
        }
    }

    #[tokio::test]
    async fn refresh_updates_existing_auth_handles() {
        let auth = BearerAuth::refreshing(
            Arc::new(CountingSource(AtomicUsize::new(0))),
            RefreshingAuthConfig {
                refresh_interval: Duration::from_millis(10),
                retry_interval: Duration::from_millis(5),
            },
        )
        .await
        .unwrap();
        let existing_handle = auth.clone();
        assert_eq!(
            existing_handle.authorization_header().unwrap(),
            "Bearer token-0"
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if existing_handle.authorization_header().unwrap() != "Bearer token-0" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert_ne!(
            existing_handle.authorization_header().unwrap(),
            "Bearer token-0"
        );
    }

    #[derive(Debug)]
    struct HangingSource;

    #[async_trait]
    impl AccessTokenSource for HangingSource {
        async fn access_token(&self) -> Result<String, Error> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn refresh_source_calls_can_be_bounded() {
        let source = HangingSource;
        assert!(fetch_refresh_token(&source, Duration::from_millis(10))
            .await
            .is_none());
    }
}
