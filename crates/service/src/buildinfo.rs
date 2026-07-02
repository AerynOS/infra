// SPDX-FileCopyrightText: 2026 AerynOS Developers
// SPDX-License-Identifier: MPL-2.0

//! Get build info for the compiled service binary

/// Git SHA
pub fn git_sha() -> &'static str {
    env!("VERGEN_GIT_SHA")
}

/// Git dirty flag
pub fn git_dirty() -> &'static str {
    env!("VERGEN_GIT_DIRTY")
}

/// Build time
pub fn build_time() -> &'static str {
    env!("VERGEN_BUILD_TIMESTAMP")
}

/// Version
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Full version string
pub fn full_version() -> String {
    let git = if let Some(sha) = option_env!("VERGEN_GIT_SHA") {
        let dirty = if git_dirty() == "true" { "-dirty" } else { "" };
        format!(" (Git ref {sha}{dirty})")
    } else {
        "".to_owned()
    };

    format!("version v{}{git} (Built at {})", version(), build_time())
}
