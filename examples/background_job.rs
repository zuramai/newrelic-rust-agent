//! Background job using the explicit API (no tracing integration).
//!
//! ```sh
//! NEW_RELIC_LICENSE_KEY=... NEW_RELIC_APP_NAME=my-jobs cargo run --example background_job
//! ```

use std::time::Duration;

use newrelic_rust_agent::{Agent, TransactionKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = Agent::from_env()?;
    // Short-lived process: connect first so distributed-trace headers are available.
    agent.wait_for_connection(Duration::from_secs(10)).await;

    let txn = agent.start_transaction("nightly-sync", TransactionKind::Other);
    {
        let fetch = txn.start_segment("fetch");
        tokio::time::sleep(Duration::from_millis(30)).await;
        {
            let _parse = fetch.start_segment("parse");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        fetch.end();
    }
    agent.record_custom_event("JobRun", [("items", 42)])?;
    agent.record_custom_metric("job/items", 42.0);
    txn.end();

    agent.shutdown(Duration::from_secs(10)).await;
    Ok(())
}
