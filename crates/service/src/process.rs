//! Execute commands
use std::{
    io,
    process::{ExitStatus, Stdio},
};

use tokio::process::{ChildStderr, ChildStdout, Command};
use tracing::trace;

/// Execute the command and return it's stdout output
pub async fn output(command: impl AsRef<str>, f: impl FnOnce(&mut Command) -> &mut Command) -> Result<String, Error> {
    let command = command.as_ref();

    let mut process = Command::new(command);

    let output = f(&mut process)
        .output()
        .await
        .map_err(|err| Error::Io(command.to_string(), err))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    trace!(command, stdout, stderr, status = %output.status, "Command finished");

    if !output.status.success() {
        Err(Error::FailedWithOutput(command.to_string(), output.status, stderr))
    } else {
        Ok(stdout)
    }
}

/// Wait for the command to finish and pipe its stdout / stderr to the provided async closure
pub async fn piped<F>(
    command: impl AsRef<str>,
    f: impl FnOnce(&mut Command) -> &mut Command,
    piped: impl FnOnce(ChildStdout, ChildStderr) -> F,
) -> Result<(), Error>
where
    F: Future<Output = ()> + Send + Sync + 'static,
{
    let command = command.as_ref();

    let mut process = Command::new(command);

    let mut child = f(&mut process)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| Error::Io(command.to_string(), err))?;

    let stdout = child.stdout.take().expect("stdout set explicitly");
    let stderr = child.stderr.take().expect("stderr set explicitly");

    let task = tokio::spawn(piped(stdout, stderr));

    let status = child.wait().await.map_err(|err| Error::Io(command.to_string(), err))?;

    let _ = task.await;

    trace!(command, %status, "Command finished");

    if !status.success() {
        Err(Error::Failed(command.to_string(), status))
    } else {
        Ok(())
    }
}

/// A process error
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An unexpected IO error occurred
    #[error("{0} failed: {1}")]
    Io(String, io::Error),
    /// Process failed
    #[error("{0} failed: {1}")]
    Failed(String, ExitStatus),
    /// Process failed with provided stderr
    #[error("{0} failed: {1}: {2}")]
    FailedWithOutput(String, ExitStatus, String),
}
