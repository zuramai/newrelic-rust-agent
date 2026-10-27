//! Optional integrations with the `tracing`, tower and reqwest ecosystems.

#[cfg(feature = "reqwest")]
pub(crate) mod reqwest;
#[cfg(feature = "tower")]
pub(crate) mod tower;
#[cfg(feature = "tracing")]
pub(crate) mod tracing;
