//! tower integration: one web transaction per HTTP request (axum, tonic, hyper, …).

use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use pin_project_lite::pin_project;

use crate::{Agent, NoticedError, Transaction, TransactionKind, TransportType};

#[cfg(feature = "tracing")]
type Inner<F> = tracing::instrument::Instrumented<F>;
#[cfg(not(feature = "tracing"))]
type Inner<F> = F;

/// A tower [`Layer`](tower_layer::Layer) that wraps every request in a web transaction.
///
/// Per request it:
/// * names the transaction `"{METHOD} {route}"` when axum's `MatchedPath` is available
///   (feature `axum`), the gRPC method path (`/pkg.Service/Method`) for gRPC requests, and
///   the bare HTTP method otherwise (rename with [`Transaction::set_name`]);
/// * accepts inbound distributed-trace headers;
/// * inserts the [`Transaction`] and its [`SegmentContext`](crate::SegmentContext) into the
///   request extensions;
/// * with feature `tracing`, runs the inner service inside a span linked to the transaction so
///   `#[tracing::instrument]` spans become segments;
/// * records the status code: HTTP `>= 500` and gRPC `UNKNOWN`, `DEADLINE_EXCEEDED`,
///   `UNIMPLEMENTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS` count as errors;
/// * ends the transaction when the response body finishes (or is dropped).
#[derive(Clone, Debug)]
pub struct TransactionLayer {
    agent: Agent,
    ignored_paths: Arc<[Cow<'static, str>]>,
}

impl TransactionLayer {
    /// Creates the layer.
    #[must_use]
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            ignored_paths: Arc::new([]),
        }
    }

    /// Does not create transactions for requests whose path equals `path` exactly (health
    /// checks, metrics endpoints).
    #[must_use]
    pub fn ignore_path(self, path: impl Into<Cow<'static, str>>) -> Self {
        let mut paths: Vec<_> = self.ignored_paths.iter().cloned().collect();
        paths.push(path.into());
        Self {
            agent: self.agent,
            ignored_paths: paths.into(),
        }
    }
}

impl<S> tower_layer::Layer<S> for TransactionLayer {
    type Service = TransactionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TransactionService {
            inner,
            layer: self.clone(),
        }
    }
}

/// The service produced by [`TransactionLayer`].
#[derive(Clone, Debug)]
pub struct TransactionService<S> {
    inner: S,
    layer: TransactionLayer,
}

fn transaction_name<B>(req: &Request<B>, is_grpc: bool) -> Cow<'static, str> {
    #[cfg(feature = "axum")]
    if let Some(path) = req.extensions().get::<axum::extract::MatchedPath>() {
        return Cow::Owned(format!("{} {}", req.method(), path.as_str()));
    }
    if is_grpc {
        return Cow::Owned(req.uri().path().to_owned());
    }
    match *req.method() {
        http::Method::GET => Cow::Borrowed("GET"),
        http::Method::POST => Cow::Borrowed("POST"),
        http::Method::PUT => Cow::Borrowed("PUT"),
        http::Method::DELETE => Cow::Borrowed("DELETE"),
        http::Method::PATCH => Cow::Borrowed("PATCH"),
        _ => Cow::Owned(req.method().as_str().to_owned()),
    }
}

impl<S, ReqB, ResB> tower_service::Service<Request<ReqB>> for TransactionService<S>
where
    S: tower_service::Service<Request<ReqB>, Response = Response<ResB>>,
    ResB: Body,
{
    type Response = Response<TransactionBody<ResB>>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<ReqB>) -> Self::Future {
        let path = req.uri().path();
        let ignored =
            !self.layer.agent.is_enabled() || self.layer.ignored_paths.iter().any(|p| p == path);
        let is_grpc = req
            .headers()
            .get(http::header::CONTENT_TYPE)
            .is_some_and(|v| v.as_bytes().starts_with(b"application/grpc"));
        let txn = if ignored {
            None
        } else {
            let txn = self
                .layer
                .agent
                .start_transaction(transaction_name(&req, is_grpc), TransactionKind::Web);
            txn.set_web_request(&req);
            txn.accept_distributed_trace_headers(req.headers(), TransportType::Http);
            req.extensions_mut().insert(txn.clone());
            req.extensions_mut().insert(txn.context());
            Some(txn)
        };
        #[cfg(feature = "tracing")]
        let inner = {
            use crate::NewRelicSpanExt as _;
            use tracing::Instrument as _;
            let span = match &txn {
                Some(txn) => {
                    let span =
                        tracing::info_span!(target: "newrelic_rust_agent", "newrelic.transaction");
                    span.set_newrelic_transaction(txn);
                    span
                }
                None => tracing::Span::none(),
            };
            self.inner.call(req).instrument(span)
        };
        #[cfg(not(feature = "tracing"))]
        let inner = self.inner.call(req);
        ResponseFuture {
            inner,
            txn,
            is_grpc,
        }
    }
}

pin_project! {
    /// Future returned by [`TransactionService`].
    pub struct ResponseFuture<F> {
        #[pin]
        inner: Inner<F>,
        txn: Option<Transaction>,
        is_grpc: bool,
    }
}

impl<F> std::fmt::Debug for ResponseFuture<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseFuture")
            .field("txn", &self.txn)
            .field("is_grpc", &self.is_grpc)
            .finish_non_exhaustive()
    }
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Body,
{
    type Output = Result<Response<TransactionBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let result = ready!(this.inner.poll(cx));
        let txn = this.txn.take();
        match result {
            Ok(resp) => {
                if let Some(txn) = &txn {
                    record_response(txn, &resp, *this.is_grpc);
                }
                let (parts, body) = resp.into_parts();
                Poll::Ready(Ok(Response::from_parts(
                    parts,
                    TransactionBody {
                        inner: body,
                        txn,
                        is_grpc: *this.is_grpc,
                    },
                )))
            }
            Err(e) => {
                if let Some(txn) = txn {
                    txn.notice_error_with(NoticedError {
                        class: "ServiceError".to_owned(),
                        message: "inner service returned an error".to_owned(),
                        expected: false,
                    });
                    txn.end();
                }
                Poll::Ready(Err(e))
            }
        }
    }
}

fn record_response<B>(txn: &Transaction, resp: &Response<B>, is_grpc: bool) {
    txn.set_status_code(resp.status().as_u16());
    let headers = resp.headers();
    if let Some(ct) = headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        txn.set_agent_attribute("response.headers.contentType", ct);
    }
    if let Some(len) = headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
    {
        txn.set_agent_attribute("response.headers.contentLength", len);
    }
    // Trailers-only gRPC responses carry the status in the headers.
    if is_grpc {
        apply_grpc_status(txn, headers);
    }
}

/// Records `grpcStatusCode` and notices server-fault gRPC codes as errors.
fn apply_grpc_status(txn: &Transaction, headers: &HeaderMap) {
    let Some(code) = headers
        .get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<i64>().ok())
    else {
        return;
    };
    txn.set_agent_attribute("grpcStatusCode", code);
    let class = match code {
        2 => "Unknown",
        4 => "DeadlineExceeded",
        12 => "Unimplemented",
        13 => "Internal",
        14 => "Unavailable",
        15 => "DataLoss",
        _ => return,
    };
    let message = headers
        .get("grpc-message")
        .map(|v| percent_decode(v.as_bytes()))
        .filter(|m| !m.is_empty())
        .unwrap_or_else(|| class.to_owned());
    txn.notice_error_with(NoticedError {
        class: class.to_owned(),
        message,
        expected: false,
    });
}

/// Decodes gRPC's percent-encoded `grpc-message`.
fn percent_decode(input: &[u8]) -> String {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%'
            && let (Some(h), Some(l)) = (
                input.get(i + 1).copied().and_then(hex),
                input.get(i + 2).copied().and_then(hex),
            )
        {
            out.push(h * 16 + l);
            i += 3;
            continue;
        }
        out.push(input[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pin_project! {
    /// Response body that ends the transaction when the body completes or is dropped, and
    /// inspects gRPC trailers.
    pub struct TransactionBody<B> {
        #[pin]
        inner: B,
        txn: Option<Transaction>,
        is_grpc: bool,
    }

    impl<B> PinnedDrop for TransactionBody<B> {
        fn drop(this: Pin<&mut Self>) {
            if let Some(txn) = this.project().txn.take() {
                txn.end();
            }
        }
    }
}

impl<B> std::fmt::Debug for TransactionBody<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBody")
            .field("txn", &self.txn)
            .field("is_grpc", &self.is_grpc)
            .finish_non_exhaustive()
    }
}

impl<B: Body> Body for TransactionBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let frame = ready!(this.inner.poll_frame(cx));
        match &frame {
            Some(Ok(f)) => {
                if let (true, Some(trailers), Some(txn)) =
                    (*this.is_grpc, f.trailers_ref(), this.txn.as_ref())
                {
                    apply_grpc_status(txn, trailers);
                }
            }
            Some(Err(_)) | None => {
                if let Some(txn) = this.txn.take() {
                    txn.end();
                }
            }
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::percent_decode;

    #[test]
    fn percent_decode_decodes_escapes_and_keeps_invalid_ones() {
        assert_eq!(percent_decode(b"bad%20thing%2"), "bad thing%2");
    }
}
