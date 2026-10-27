//! `reqwest-middleware` integration: External segments and outbound distributed tracing.

use http::Extensions;
use reqwest_middleware::{Middleware, Next};

use crate::SegmentContext;

/// Times outbound requests as External segments and adds distributed-trace headers.
///
/// The parent is taken from a [`SegmentContext`] in the request extensions (see
/// `reqwest_middleware::RequestBuilder::with_extension`) or, with feature `tracing`, from the
/// current span. Requests made outside a transaction pass through untouched.
///
/// ```no_run
/// # fn build() {
/// let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
///     .with(newrelic_rust_agent::NewRelicMiddleware)
///     .build();
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct NewRelicMiddleware;

fn current_context() -> Option<SegmentContext> {
    #[cfg(feature = "tracing")]
    {
        use crate::NewRelicSpanExt as _;
        tracing::Span::current().newrelic_context()
    }
    #[cfg(not(feature = "tracing"))]
    {
        None
    }
}

#[async_trait::async_trait]
impl Middleware for NewRelicMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let ctx = extensions
            .get::<SegmentContext>()
            .cloned()
            .or_else(current_context);
        let Some(ctx) = ctx else {
            return next.run(req, extensions).await;
        };
        let Ok(uri) = req.url().as_str().parse::<http::Uri>() else {
            return next.run(req, extensions).await;
        };
        let segment = ctx.start_external_segment(req.method(), &uri);
        segment.insert_distributed_trace_headers(req.headers_mut());
        let result = next.run(req, extensions).await;
        if let Ok(resp) = &result {
            segment.set_status_code(resp.status().as_u16());
        }
        segment.end();
        result
    }
}
