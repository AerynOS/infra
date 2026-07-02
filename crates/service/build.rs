use std::path::Path;

use vergen_gitcl::{BuildBuilder, Emitter, GitclBuilder};

fn main() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");

    for path in [crates.join("avalanche"), crates.join("summit"), crates.join("vessel")] {
        println!("cargo::rerun-if-changed={}", path.display());
    }

    let build = BuildBuilder::all_build().unwrap();
    let gitcl = GitclBuilder::all_git().unwrap();

    Emitter::default()
        .add_instructions(&build)
        .unwrap()
        .add_instructions(&gitcl)
        .unwrap()
        .emit()
        .expect("emit vergen instructions");
}
