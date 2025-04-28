//! Execute commands
use std::{io, process::Stdio};

use tokio::process::{ChildStderr, ChildStdout, Command};

/// Execute the command and return it's stdout output
pub async fn output(command: impl AsRef<str>, f: impl FnOnce(&mut Command) -> &mut Command) -> Result<String, Error> {
    let command = command.as_ref();

    let mut process = Command::new(command);

    let output = f(&mut process)
        .output()
        .await
        .map_err(|err| Error::Io(command.to_string(), err))?;

    if !output.status.success() {
        let error = String::from_utf8_lossy(&output.stderr).to_string();

        if let Some(code) = output.status.code() {
            Err(Error::FailedOutputWithStatus(command.to_string(), code, error))
        } else {
            Err(Error::FailedOutput(command.to_string(), error))
        }
    } else {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }
}

/// Execute the command, discarding its output
pub async fn execute(command: impl AsRef<str>, f: impl FnOnce(&mut Command) -> &mut Command) -> Result<(), Error> {
    let command = command.as_ref();

    let mut process = Command::new(command);

    let status = f(&mut process)
        .status()
        .await
        .map_err(|err| Error::Io(command.to_string(), err))?;

    if !status.success() {
        if let Some(code) = status.code() {
            Err(Error::FailedWithStatus(command.to_string(), code))
        } else {
            Err(Error::Failed(command.to_string()))
        }
    } else {
        Ok(())
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

    if !status.success() {
        if let Some(code) = status.code() {
            Err(Error::FailedWithStatus(command.to_string(), code))
        } else {
            Err(Error::Failed(command.to_string()))
        }
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
    #[error("{0} exited with failure")]
    Failed(String),
    /// Process failed with exit code
    #[error("{0} failed with exit status {1}")]
    FailedWithStatus(String, i32),
    /// Process failed with provided stderr
    #[error("{0} exited with failure: {1}")]
    FailedOutput(String, String),
    /// Process failed with exit code & provided stderr
    #[error("{0} failed with exit status {1}: {2}")]
    FailedOutputWithStatus(String, i32, String),
}
