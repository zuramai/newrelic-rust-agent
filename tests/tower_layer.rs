//! `TransactionLayer` creates web transactions for axum and gRPC-style services.
#![cfg(all(feature = "axum", feature = "tracing"))]
#![expect(
    clippy::unwrap_used,
    reason = "test helpers panic on unexpected service failures"
)]

mod common;

use std::convert::Infallible;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::routing::get;
use common::{events, fake_collector, has_metric, metric_names};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt as _;
use newrelic_rust_agent::{Transaction, TransactionLayer};
use tower::ServiceExt as _;

async fn user(axum::extract::Path(id): axum::extract::Path<u64>) -> String {
    format!("user {id}")
}

async fn fail() -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

fn router(agent: newrelic_rust_agent::Agent) -> Router {
    Router::new()
        .route("/users/{id}", get(user))
        .route("/fail", get(fail))
        .route(
            "/renamed",
            get(
                |axum::Extension(txn): axum::Extension<Transaction>| async move {
                    txn.set_name("custom-name");
                    "ok"
                },
            ),
        )
        .route("/health", get(|| async { "ok" }))
        .layer(TransactionLayer::new(agent).ignore_path("/health"))
}

async fn call(app: Router, uri: &str) -> StatusCode {
    let resp = app
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    resp.into_body().collect().await.unwrap();
    status
}

#[tokio::test]
async fn axum_route_template_names_the_transaction() {
    let (server, agent) = fake_collector().await;
    assert_eq!(
        call(router(agent.clone()), "/users/42").await,
        StatusCode::OK
    );
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "WebTransaction/Rust/GET /users/{id}",
        None
    ));
    assert!(has_metric(&names, "HttpDispatcher", None));
    let txn = &events(&server, "analytic_event_data").await[0];
    assert_eq!(txn[2]["request.uri"], "/users/42");
    assert_eq!(txn[2]["http.statusCode"], 200);
}

#[tokio::test]
async fn http_500_records_transaction_error() {
    let (server, agent) = fake_collector().await;
    call(router(agent.clone()), "/fail").await;
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(has_metric(
        &metric_names(&server).await,
        "Errors/allWeb",
        None
    ));
    let errors = events(&server, "error_event_data").await;
    assert_eq!(errors[0][0]["error.class"], "500");
}

#[tokio::test]
async fn handler_can_rename_transaction_via_extension() {
    let (server, agent) = fake_collector().await;
    call(router(agent.clone()), "/renamed").await;
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(has_metric(
        &metric_names(&server).await,
        "WebTransaction/Rust/custom-name",
        None
    ));
}

#[tokio::test]
async fn ignored_path_creates_no_transaction() {
    let (server, agent) = fake_collector().await;
    call(router(agent.clone()), "/health").await;
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(events(&server, "analytic_event_data").await.is_empty());
}

#[tokio::test]
async fn grpc_internal_status_in_trailers_records_error() {
    let (server, agent) = fake_collector().await;
    let svc = tower::service_fn(|_req: Request<Body>| async {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "13".parse().unwrap());
        trailers.insert("grpc-message", "db%20down".parse().unwrap());
        let body = http_body_util::StreamBody::new(futures_stream(trailers));
        Ok::<_, Infallible>(
            Response::builder()
                .header("content-type", "application/grpc")
                .body(body)
                .unwrap(),
        )
    });
    let svc = tower::ServiceBuilder::new()
        .layer(TransactionLayer::new(agent.clone()))
        .service(svc);
    let req = Request::post("/face.FaceService/Detect")
        .header("content-type", "application/grpc")
        .body(Body::empty())
        .unwrap();
    let resp = svc.oneshot(req).await.unwrap();
    resp.into_body().collect().await.unwrap();
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "WebTransaction/Rust/face.FaceService/Detect",
        None
    ));
    assert!(has_metric(&names, "Errors/allWeb", None));
    let errors = events(&server, "error_event_data").await;
    assert_eq!(errors[0][0]["error.class"], "Internal");
    assert_eq!(errors[0][0]["error.message"], "db down");
}

#[tokio::test]
async fn grpc_ok_status_is_not_an_error() {
    let (server, agent) = fake_collector().await;
    let svc = tower::service_fn(|_req: Request<Body>| async {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "5".parse().unwrap());
        let body = http_body_util::StreamBody::new(futures_stream(trailers));
        Ok::<_, Infallible>(Response::new(body))
    });
    let svc = tower::ServiceBuilder::new()
        .layer(TransactionLayer::new(agent.clone()))
        .service(svc);
    let req = Request::post("/pkg.Svc/Get")
        .header("content-type", "application/grpc")
        .body(Body::empty())
        .unwrap();
    svc.oneshot(req)
        .await
        .unwrap()
        .into_body()
        .collect()
        .await
        .unwrap();
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(!has_metric(
        &metric_names(&server).await,
        "Errors/allWeb",
        None
    ));
    let txn = &events(&server, "analytic_event_data").await[0];
    assert_eq!(txn[2]["grpcStatusCode"], 5);
}

/// A body stream yielding one data frame then the trailers.
fn futures_stream(
    trailers: http::HeaderMap,
) -> impl futures_util::Stream<Item = Result<http_body::Frame<bytes::Bytes>, Infallible>> {
    let frames = vec![
        Ok(http_body::Frame::data(bytes::Bytes::from_static(
            b"\0\0\0\0\0",
        ))),
        Ok(http_body::Frame::trailers(trailers)),
    ];
    futures_util::stream::iter(frames)
}
