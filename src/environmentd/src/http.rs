// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Embedded HTTP server.
//!
//! environmentd embeds an HTTP server for introspection into the running
//! process. At the moment, its primary exports are Prometheus metrics, heap
//! profiles, and catalog dumps.

// Axum handlers must use async, but often don't actually use `await`.
#![allow(clippy::unused_async)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{FromRequest, RequestParts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{routing, Extension, Router};
use futures::future::{FutureExt, Shared, TryFutureExt};
use headers::authorization::{Authorization, Basic, Bearer};
use headers::{HeaderMapExt, HeaderName};
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use http::{Request, StatusCode};
use hyper_openssl::MaybeHttpsStream;
use openssl::nid::Nid;
use openssl::ssl::{Ssl, SslContext};
use openssl::x509::X509;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_openssl::SslStream;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tracing::{error, warn};

use mz_adapter::catalog::{HTTP_DEFAULT_USER, SYSTEM_USER};
use mz_adapter::session::{ExternalUserMetadata, Session, User};
use mz_adapter::SessionClient;
use mz_frontegg_auth::{FronteggAuthentication, FronteggError};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingTargetCallbacks;

use crate::server::{ConnectionHandler, Server};
use crate::BUILD_INFO;

mod catalog;
mod memory;
mod root;
mod sql;

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub tls: Option<TlsConfig>,
    pub frontegg: Option<FronteggAuthentication>,
    pub adapter_client: mz_adapter::Client,
    pub allowed_origin: AllowOrigin,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub context: SslContext,
    pub mode: TlsMode,
}

#[derive(Debug, Clone, Copy)]
pub enum TlsMode {
    Require,
    AssumeUser,
}

#[derive(Debug)]
pub struct HttpServer {
    tls: Option<TlsConfig>,
    router: Router,
}

impl HttpServer {
    pub fn new(
        HttpConfig {
            tls,
            frontegg,
            adapter_client,
            allowed_origin,
        }: HttpConfig,
    ) -> HttpServer {
        let tls_mode = tls.as_ref().map(|tls| tls.mode);
        let frontegg = Arc::new(frontegg);
        let (adapter_client_tx, adapter_client_rx) = oneshot::channel();
        adapter_client_tx
            .send(adapter_client)
            .expect("rx known to be live");
        let router = base_router(BaseRouterConfig { profiling: false })
            .layer(middleware::from_fn(move |req, next| {
                let frontegg = Arc::clone(&frontegg);
                async move { auth(req, next, tls_mode, &frontegg).await }
            }))
            .layer(Extension(adapter_client_rx.shared()))
            .layer(
                CorsLayer::new()
                    .allow_credentials(false)
                    .allow_headers([
                        AUTHORIZATION,
                        CONTENT_TYPE,
                        HeaderName::from_static("x-materialize-version"),
                    ])
                    .allow_methods(Any)
                    .allow_origin(allowed_origin)
                    .expose_headers(Any)
                    .max_age(Duration::from_secs(60) * 60),
            );
        HttpServer { tls, router }
    }

    fn tls_context(&self) -> Option<&SslContext> {
        self.tls.as_ref().map(|tls| &tls.context)
    }
}

impl Server for HttpServer {
    const NAME: &'static str = "http";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        let router = self.router.clone();
        let tls_context = self.tls_context().cloned();
        Box::pin(async {
            let (conn, conn_protocol) = match tls_context {
                Some(tls_context) => {
                    let mut ssl_stream = SslStream::new(Ssl::new(&tls_context)?, conn)?;
                    if let Err(e) = Pin::new(&mut ssl_stream).accept().await {
                        let _ = ssl_stream.get_mut().shutdown().await;
                        return Err(e.into());
                    }
                    let client_cert = ssl_stream.ssl().peer_certificate();
                    (
                        MaybeHttpsStream::Https(ssl_stream),
                        ConnProtocol::Https { client_cert },
                    )
                }
                _ => (MaybeHttpsStream::Http(conn), ConnProtocol::Http),
            };
            let svc = router.layer(Extension(conn_protocol));
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, svc).err_into().await
        })
    }
}

pub struct InternalHttpConfig {
    pub metrics_registry: MetricsRegistry,
    pub tracing_target_callbacks: TracingTargetCallbacks,
    pub adapter_client_rx: oneshot::Receiver<mz_adapter::Client>,
}

pub struct InternalHttpServer {
    router: Router,
}

impl InternalHttpServer {
    pub fn new(
        InternalHttpConfig {
            metrics_registry,
            tracing_target_callbacks,
            adapter_client_rx,
        }: InternalHttpConfig,
    ) -> InternalHttpServer {
        let router = base_router(BaseRouterConfig { profiling: true })
            .route(
                "/metrics",
                routing::get(move || async move {
                    mz_http_util::handle_prometheus(&metrics_registry).await
                }),
            )
            .route(
                "/api/livez",
                routing::get(mz_http_util::handle_liveness_check),
            )
            .route(
                "/api/opentelemetry/config",
                routing::put(move |payload| async move {
                    mz_http_util::handle_modify_filter_target(
                        tracing_target_callbacks.tracing,
                        payload,
                    )
                    .await
                }),
            )
            .route(
                "/api/stderr/config",
                routing::put(move |payload| async move {
                    mz_http_util::handle_modify_filter_target(
                        tracing_target_callbacks.stderr,
                        payload,
                    )
                    .await
                }),
            )
            .route(
                "/api/catalog",
                routing::get(catalog::handle_internal_catalog),
            )
            .layer(Extension(AuthedUser {
                user: SYSTEM_USER.clone(),
                create_if_not_exists: false,
            }))
            .layer(Extension(adapter_client_rx.shared()));
        InternalHttpServer { router }
    }
}

#[async_trait]
impl Server for InternalHttpServer {
    const NAME: &'static str = "internal_http";

    fn handle_connection(&self, conn: TcpStream) -> ConnectionHandler {
        let router = self.router.clone();
        Box::pin(async {
            let http = hyper::server::conn::Http::new();
            http.serve_connection(conn, router).err_into().await
        })
    }
}

type Delayed<T> = Shared<oneshot::Receiver<T>>;

#[derive(Clone)]
enum ConnProtocol {
    Http,
    Https { client_cert: Option<X509> },
}

#[derive(Clone)]
struct AuthedUser {
    user: User,
    create_if_not_exists: bool,
}

pub struct AuthedClient(pub SessionClient);

#[async_trait]
impl<B> FromRequest<B> for AuthedClient
where
    B: Send,
{
    type Rejection = (StatusCode, String);

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        let AuthedUser {
            user,
            create_if_not_exists,
        } = req.extensions().get::<AuthedUser>().unwrap();
        let adapter_client = req
            .extensions()
            .get::<Delayed<mz_adapter::Client>>()
            .unwrap()
            .clone();

        let adapter_client = adapter_client
            .await
            .map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "adapter client missing".into(),
                )
            })?
            .new_conn()
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let session = Session::new(adapter_client.conn_id(), user.clone());
        let (adapter_client, _) = match adapter_client.startup(session, *create_if_not_exists).await
        {
            Ok(adapter_client) => adapter_client,
            Err(e) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
            }
        };

        Ok(AuthedClient(adapter_client))
    }
}

#[derive(Debug, Error)]
enum AuthError {
    #[error("HTTPS is required")]
    HttpsRequired,
    #[error("invalid username in client certificate")]
    InvalidCertUserName,
    #[error("unauthorized login to user '{0}'")]
    InvalidLogin(String),
    #[error("{0}")]
    Frontegg(#[from] FronteggError),
    #[error("missing authorization header")]
    MissingHttpAuthentication,
    #[error("{0}")]
    MismatchedUser(&'static str),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        warn!("HTTP request failed authentication: {}", self);
        // We omit most detail from the error message we send to the client, to
        // avoid giving attackers unnecessary information.
        let message = match self {
            AuthError::HttpsRequired => self.to_string(),
            _ => "unauthorized".into(),
        };
        (
            StatusCode::UNAUTHORIZED,
            [(http::header::WWW_AUTHENTICATE, "Basic realm=Materialize")],
            message,
        )
            .into_response()
    }
}

async fn auth<B>(
    mut req: Request<B>,
    next: Next<B>,
    tls_mode: Option<TlsMode>,
    frontegg: &Option<FronteggAuthentication>,
) -> impl IntoResponse {
    // There are three places a username may be specified:
    //
    //   - certificate common name
    //   - HTTP Basic authentication
    //   - JWT email address
    //
    // We verify that if any of these are present, they must match any other
    // that is also present.

    // First, extract the username from the certificate, validating that the
    // connection matches the TLS configuration along the way.
    let conn_protocol = req.extensions().get::<ConnProtocol>().unwrap();
    let mut user = match (tls_mode, &conn_protocol) {
        (None, ConnProtocol::Http) => None,
        (None, ConnProtocol::Https { .. }) => unreachable!(),
        (Some(TlsMode::Require), ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (Some(TlsMode::Require), ConnProtocol::Https { .. }) => None,
        (Some(TlsMode::AssumeUser), ConnProtocol::Http) => return Err(AuthError::HttpsRequired),
        (Some(TlsMode::AssumeUser), ConnProtocol::Https { client_cert }) => client_cert
            .as_ref()
            .and_then(|cert| cert.subject_name().entries_by_nid(Nid::COMMONNAME).next())
            .and_then(|cn| cn.data().as_utf8().ok())
            .map(|cn| Some(cn.to_string()))
            .ok_or(AuthError::InvalidCertUserName)?,
    };

    // Then, handle Frontegg authentication if required.
    let user = match frontegg {
        // If no Frontegg authentication, we can use the cert's username if
        // present, otherwise the default HTTP user.
        None => User {
            name: user.unwrap_or_else(|| HTTP_DEFAULT_USER.name.to_string()),
            external_metadata: None,
        },
        // If we require Frontegg auth, fetch credentials from the HTTP auth
        // header. Basic auth comes with a username/password, where the password
        // is the client+secret pair. Bearer auth is an existing JWT that must
        // be validated. In either case, if a username was specified in the
        // client cert, it must match that of the JWT.
        Some(frontegg) => {
            let token = if let Some(basic) = req.headers().typed_get::<Authorization<Basic>>() {
                if let Some(user) = user {
                    if basic.username() != user {
                        return Err(AuthError::MismatchedUser(
                            "user in client certificate did not match user specified in authorization header",
                        ));
                    }
                }
                user = Some(basic.username().to_string());
                frontegg
                    .exchange_password_for_token(basic.0.password())
                    .await?
                    .access_token
            } else if let Some(bearer) = req.headers().typed_get::<Authorization<Bearer>>() {
                bearer.token().to_string()
            } else {
                return Err(AuthError::MissingHttpAuthentication);
            };
            let claims = frontegg.validate_access_token(&token, user.as_deref())?;
            User {
                external_metadata: Some(ExternalUserMetadata {
                    user_id: claims.best_user_id(),
                    group_id: claims.tenant_id,
                }),
                name: claims.email,
            }
        }
    };

    if mz_adapter::catalog::is_reserved_name(user.name.as_str()) {
        return Err(AuthError::InvalidLogin(user.name));
    }

    // Add the authenticated user as an extension so downstream handlers can
    // inspect it if necessary.
    req.extensions_mut().insert(AuthedUser {
        user,
        create_if_not_exists: frontegg.is_some() || !matches!(tls_mode, Some(TlsMode::AssumeUser)),
    });

    // Run the request.
    Ok(next.run(req).await)
}

/// Configuration for [`base_router`].
struct BaseRouterConfig {
    /// Whether to enable the profiling routes.
    profiling: bool,
}

/// Returns the router for routes that are shared between the internal and
/// external HTTP servers.
fn base_router(BaseRouterConfig { profiling }: BaseRouterConfig) -> Router {
    let mut router = Router::new()
        .route(
            "/",
            routing::get(move || async move { root::handle_home(profiling).await }),
        )
        .route("/api/sql", routing::post(sql::handle_sql))
        .route("/memory", routing::get(memory::handle_memory))
        .route(
            "/hierarchical-memory",
            routing::get(memory::handle_hierarchical_memory),
        )
        .route("/static/*path", routing::get(root::handle_static));
    if profiling {
        router = router.nest("/prof/", mz_prof::http::router(&BUILD_INFO));
    }
    router
}
