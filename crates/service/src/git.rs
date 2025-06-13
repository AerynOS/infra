//! Common git commands
use std::path::Path;

use http::Uri;

use crate::process;

/// git clone --mirror -- uri to
pub async fn mirror(uri: &Uri, to: &Path) -> Result<(), process::Error> {
    process::output("git", |process| {
        process.args(["clone", "--mirror", "--"]).arg(uri.to_string()).arg(to)
    })
    .await?;

    Ok(())
}

/// git remote update
pub async fn remote_update(path: &Path) -> Result<(), process::Error> {
    process::output("git", |process| process.args(["remote", "update"]).current_dir(path)).await?;

    Ok(())
}

/// git worktree add worktree_dir commit_ref
pub async fn checkout_worktree(source_dir: &Path, worktree_dir: &Path, commit_ref: &str) -> Result<(), process::Error> {
    process::output("git", |process| {
        process
            .args(["worktree", "add", "-f"])
            .arg(worktree_dir)
            .arg(commit_ref)
            .current_dir(source_dir)
    })
    .await?;

    Ok(())
}

/// git worktree remove worktree_dir
pub async fn remove_worktree(source_dir: &Path, worktree_dir: &Path) -> Result<(), process::Error> {
    process::output("git", |process| {
        process
            .args(["worktree", "remove"])
            .arg(worktree_dir)
            .current_dir(source_dir)
    })
    .await?;

    Ok(())
}

/// git rev-parse arg
pub async fn rev_parse(source_dir: &Path, arg: &str) -> Result<String, process::Error> {
    let output = process::output("git", |process| {
        process.args(["rev-parse", arg]).current_dir(source_dir)
    })
    .await?;

    Ok(output.trim().to_owned())
}
