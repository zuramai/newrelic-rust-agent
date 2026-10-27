//! axum server reporting to New Relic APM.
//!
//! ```sh
//! NEW_RELIC_LICENSE_KEY=... NEW_RELIC_APP_NAME=newrelic-rust-agent-smoke \
//!   RUST_LOG=newrelic_rust_agent=debug,info \
//!   cargo run --example axum --features axum,tracing,reqwest
//! curl localhost:3000/users/1; curl localhost:3000/fail
//! ```

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Router};
use newrelic_rust_agent::{
    Agent, NewRelicLayer, NewRelicMiddleware, NoticedError, Transaction, TransactionLayer,
};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = Agent::from_env()?;
    tracing_subscriber::registry()
        .with(NewRelicLayer::new(agent.clone()))
        // Filter only the console output; the New Relic layer must still see INFO spans.
        .with(tracing_subscriber::fmt::layer().with_filter(console_filter()))
        .init();

    let client = ClientBuilder::new(reqwest::Client::new())
        .with(NewRelicMiddleware)
        .build();
    let app = Router::new()
        .route("/users/{id}", get(get_user))
        .route("/fail", get(fail))
        .with_state(client)
        .layer(TransactionLayer::new(agent.clone()));

    let port = std::env::var("PORT").unwrap_or_else(|_| "3000".to_owned());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("listening on http://localhost:{port}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    agent.shutdown(Duration::from_secs(10)).await;
    Ok(())
}

async fn get_user(State(client): State<ClientWithMiddleware>, Path(id): Path<u64>) -> String {
    let name = load_user(id).await;
    let upstream = client
        .get("https://example.com")
        .send()
        .await
        .map_or(0, |r| r.status().as_u16());
    format!("{name} (upstream {upstream})")
}

#[tracing::instrument]
async fn load_user(id: u64) -> String {
    tokio::time::sleep(Duration::from_millis(15)).await;
    tracing::info!(user_id = id, "user loaded");
    format!("user-{id}")
}

async fn fail(Extension(txn): Extension<Transaction>) -> StatusCode {
    txn.notice_error_with(NoticedError {
        class: "PaymentDeclined".to_owned(),
        message: "card declined".to_owned(),
        expected: false,
    });
    StatusCode::INTERNAL_SERVER_ERROR
}

/// `RUST_LOG` if set, otherwise INFO (an unset `RUST_LOG` would hide everything below ERROR).
fn console_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}
