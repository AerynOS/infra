//! Capture unix signals
use std::io;

use futures_util::{FutureExt, future};
pub use tokio::signal::unix::SignalKind as Kind;
use tokio::signal::unix::signal;

/// Returns a future that resolves when one of the provided signals is captured
pub async fn capture(signals: impl IntoIterator<Item = Kind>) -> io::Result<()> {
    let mut signals = signals.into_iter().map(signal).collect::<Result<Vec<_>, _>>()?;

    future::select_all(signals.iter_mut().map(|signal| signal.recv().boxed())).await;

    Ok(())
}
