//! Execute commands
use std::{
    io,
    process::{ExitStatus, Stdio},
};

use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tracing::trace;

/// Execute the command and return it's stdout output
pub async fn output(command: impl AsRef<str>, f: impl FnOnce(&mut Command) -> &mut Command) -> Result<String, Error> {
    let command = command.as_ref();

    let mut process = Command::new(command);

    let output = f(&mut process)
        .output()
        .await
        .map_err(|err| Error::Io(command.to_owned(), err))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    trace!(command, stdout, stderr, status = %output.status, "Command finished");

    if !output.status.success() {
        Err(Error::FailedWithOutput(command.to_owned(), output.status, stderr))
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

    let mut child = KillOnDrop(
        f(&mut process)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| Error::Io(command.to_owned(), err))?,
    );

    let stdout = child.0.stdout.take().expect("stdout set explicitly");
    let stderr = child.0.stderr.take().expect("stderr set explicitly");

    let task = tokio::spawn(piped(stdout, stderr));

    let status = child.0.wait().await.map_err(|err| Error::Io(command.to_owned(), err))?;

    let _ = task.await;

    trace!(command, %status, "Command finished");

    if !status.success() {
        Err(Error::Failed(command.to_owned(), status))
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

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}
