// Stamp the build with the current git commit so the running binary can
// report exactly what source it was built from (surfaced in the config Web
// UI's version/update panel). Falls back to placeholder values instead of
// failing the build when git isn't available (e.g. a source tarball without
// a `.git` directory) — a missing version stamp is much less disruptive than
// a broken build.
use std::process::Command;

fn main() {
    let hash = git_output(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "dev".to_string());
    let date = git_output(&["log", "-1", "--format=%cs"]).unwrap_or_default();
    println!("cargo:rustc-env=CGL_GIT_HASH={hash}");
    println!("cargo:rustc-env=CGL_GIT_DATE={date}");

    // Re-run this script (and thus refresh the stamped commit) whenever HEAD
    // moves, instead of only when source files change.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads/main");
}

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
