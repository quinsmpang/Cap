use reqwest::StatusCode;
use tauri::{Emitter, Manager, Runtime};
use thiserror::Error;
use tracing::{debug, error, warn};

use crate::{ArcLock, http_client};

#[derive(Error, Debug)]
pub enum AuthedApiError {
    #[error("User is not authenticated or credentials have expired!")]
    InvalidAuthentication,
    #[error("User needs to upgrade their account to use this feature!")]
    UpgradeRequired,
    #[error("App state is still initializing")]
    AppStateUnavailable,
    #[error("AuthedApiError/AuthStore: {0}")]
    AuthStore(String),
    #[error("AuthedApiError/Request: {0}")]
    Request(reqwest::Error),
    #[error("AuthedApiError/Deserialization: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("The request has timed out")]
    Timeout,
    #[error(
        "Cloud verification rejected the uploaded recording; local data must be uploaded again"
    )]
    ReuploadRequired,
    #[error("Recording is awaiting cloud verification; local files are retained")]
    VerificationPending,
    #[error("AuthedApiError/Other: {0}")]
    Other(String),
}

impl From<reqwest::Error> for AuthedApiError {
    fn from(err: reqwest::Error) -> Self {
        match err {
            err if err.is_timeout() => AuthedApiError::Timeout,
            err => AuthedApiError::Request(err.without_url()),
        }
    }
}

impl From<&'static str> for AuthedApiError {
    fn from(value: &'static str) -> Self {
        AuthedApiError::Other(value.into())
    }
}

impl From<String> for AuthedApiError {
    fn from(value: String) -> Self {
        AuthedApiError::Other(value)
    }
}

fn apply_env_headers(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    let mut req = req
        .header("X-Cap-Desktop-Version", env!("CARGO_PKG_VERSION"))
        .header("X-Cap-Desktop-Features", "googleDriveUpload");

    if let Ok(s) = std::env::var("VITE_VERCEL_AUTOMATION_BYPASS_SECRET") {
        req = req.header("x-vercel-protection-bypass", s);
    }

    req
}

fn default_server_url() -> String {
    option_env!("VITE_SERVER_URL")
        .unwrap_or("https://cap.so")
        .to_string()
}

async fn current_server_url<T, R>(manager: &T) -> Result<String, AuthedApiError>
where
    T: Manager<R> + ?Sized,
    R: Runtime,
{
    let Some(app_state) = manager.try_state::<ArcLock<crate::App>>() else {
        return Err(AuthedApiError::AppStateUnavailable);
    };

    Ok(app_state.read().await.server_url.clone())
}

#[derive(Clone)]
pub(crate) struct UploadRequestContext {
    server_url: String,
    owner_id: String,
}

tokio::task_local! {
    static UPLOAD_REQUEST: UploadRequestContext;
}

impl UploadRequestContext {
    pub(crate) fn new(server_url: String, owner_id: String) -> Result<Self, AuthedApiError> {
        if server_url.is_empty() || owner_id.is_empty() {
            return Err(AuthedApiError::InvalidAuthentication);
        }
        Ok(Self {
            server_url,
            owner_id,
        })
    }

    fn check_identity(
        &self,
        server_url: &str,
        owner_id: Option<&str>,
    ) -> Result<(), AuthedApiError> {
        if server_url != self.server_url || owner_id != Some(self.owner_id.as_str()) {
            return Err(AuthedApiError::InvalidAuthentication);
        }
        Ok(())
    }

    pub(crate) async fn check(&self, _app: &tauri::AppHandle) -> Result<(), AuthedApiError> {
        Err(AuthedApiError::InvalidAuthentication)
    }

    pub(crate) async fn run<F: std::future::Future>(self, future: F) -> F::Output {
        UPLOAD_REQUEST.scope(self, future).await
    }

    pub(crate) fn current() -> Option<Self> {
        UPLOAD_REQUEST.try_with(Clone::clone).ok()
    }
}

pub(crate) fn inherit_upload_context<F: std::future::Future>(
    context: Option<UploadRequestContext>,
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    let scope = crate::upload::lifecycle::UploadScope::current();
    async move {
        let request = async move {
            match context {
                Some(context) => context.run(future).await,
                None => future.await,
            }
        };
        match scope {
            Some(scope) => scope.bind(request).await,
            None => request.await,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait ManagerExt<R: Runtime>: Manager<R> {
    async fn authed_api_request(
        &self,
        path: impl Into<String>,
        build: impl FnOnce(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, AuthedApiError>;

    async fn api_request(
        &self,
        path: impl Into<String>,
        build: impl FnOnce(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error>;

    async fn make_app_url(&self, pathname: impl AsRef<str>) -> String;

    async fn is_server_url_custom(&self) -> bool;
}

impl<T: Manager<R> + Emitter<R>, R: Runtime> ManagerExt<R> for T {
    async fn authed_api_request(
        &self,
        _path: impl Into<String>,
        _build: impl FnOnce(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, AuthedApiError> {
        debug!("Skipping authenticated API request because authentication is disabled");
        Err(AuthedApiError::InvalidAuthentication)
    }

    async fn api_request(
        &self,
        path: impl Into<String>,
        build: impl FnOnce(&reqwest::Client, String) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let url = self.make_app_url(path.into()).await;

        apply_env_headers(build(&self.state::<http_client::HttpClient>(), url))
            .send()
            .await
    }

    async fn make_app_url(&self, pathname: impl AsRef<str>) -> String {
        let pathname = pathname.as_ref();
        match current_server_url(self).await {
            Ok(server_url) => format!("{server_url}{pathname}"),
            Err(err) => {
                warn!("App state unavailable while building app URL: {err}");
                format!("{}{}", default_server_url(), pathname)
            }
        }
    }

    async fn is_server_url_custom(&self) -> bool {
        match current_server_url(self).await {
            Ok(server_url) => {
                if let Some(env_url) = std::option_env!("VITE_SERVER_URL") {
                    return server_url != env_url;
                }

                false
            }
            Err(err) => {
                warn!("App state unavailable while reading server URL settings: {err}");
                false
            }
        }
    }
}

#[cfg(test)]
mod upload_context_tests {
    use super::*;

    #[test]
    fn upload_request_errors_do_not_expose_signed_urls() {
        let url = reqwest::Url::parse(
            "https://storage.invalid/object?X-Amz-Signature=private-signature&upload_id=private-session",
        )
        .unwrap();
        let error = reqwest::Client::new()
            .put(url.clone())
            .header("invalid\nheader", "value")
            .build()
            .unwrap_err()
            .with_url(url);
        assert!(error.url().is_some());
        let error = AuthedApiError::from(error);
        assert!(
            matches!(&error, AuthedApiError::Request(inner) if inner.is_builder() && inner.url().is_none())
        );
        for diagnostic in [error.to_string(), format!("{error:?}")] {
            assert!(!diagnostic.contains("private-signature"));
            assert!(!diagnostic.contains("private-session"));
            assert!(!diagnostic.contains("storage.invalid"));
        }
    }

    #[test]
    fn upload_request_identity_cannot_follow_a_changed_account_or_server() {
        let context =
            UploadRequestContext::new("https://original.invalid".into(), "owner".into()).unwrap();
        assert!(
            context
                .check_identity("https://original.invalid", Some("owner"))
                .is_ok()
        );
        assert!(
            context
                .check_identity("https://changed.invalid", Some("owner"))
                .is_err()
        );
        assert!(
            context
                .check_identity("https://original.invalid", Some("another-owner"))
                .is_err()
        );
        assert!(
            context
                .check_identity("https://original.invalid", None)
                .is_err()
        );
    }
}
