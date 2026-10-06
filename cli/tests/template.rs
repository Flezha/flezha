//! `flz template`: the templates the installed release carries, read in a VM
//! of its own and written by `flz` itself, into a folder or a tar stream.
//!
//! The VM is a shim here: it echoes the expression for `list`, and for
//! `create` prints a boot log line and then a template, the way the release's
//! answers one.

mod common;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

use common::Shim;
use tempfile::TempDir;

const FLZ_BIN: &str = env!("CARGO_BIN_EXE_flz");

const ECHOING: &str = "#!/bin/sh\nprintf '%s\\n' \"$@\"\nexit 0\n";

const TEMPLATE: &str = r##"#!/bin/sh
echo 'a boot log line'
printf '%s\n' '{"files":[{"path":".gitignore","content":"/target/\n","executable":false},{"path":"bin/run","content":"#!/bin/sh\n","executable":true},{"path":"src/a.txt","content":"a\n","executable":false}],"run":["make test","make run"]}'
"##;

/// The release's refusal of an unknown template: its rendering on stderr and
/// the exit code of its class.
const UNKNOWN: &str =
    "#!/bin/sh\necho 'error: there is no template (template.unknown)' >&2\nexit 5\n";

fn create(shim: &Shim, cwd: &Path, args: &[&str]) -> Output {
    shim.cmd()
        .current_dir(cwd)
        .args(["template", "create"])
        .args(args)
        .output()
        .expect("flz runs")
}

fn mode(path: &Path) -> u32 {
    fs::metadata(path).expect("written").permissions().mode() & 0o777
}

fn list_and_create_ask_the_vm_of_their_own_with_no_token() {
    let shim = Shim::new(FLZ_BIN, ECHOING);

    let listed = shim.run(&["template", "list", "--token", "t"]);
    let out = String::from_utf8_lossy(&listed.stdout);
    assert!(listed.status.success(), "{:?}", listed);
    assert!(out.starts_with("eval\n"), "{out}");
    assert!(
        out.contains(r#"Flezha.Mcp.Knowledge.Command.eval("list", [], [])"#),
        "{out}"
    );

    let json = shim.run(&["template", "list", "--json"]);
    assert!(
        String::from_utf8_lossy(&json.stdout)
            .contains(r#"Flezha.Mcp.Knowledge.Command.eval("list", [], [output: "json"])"#),
        "{:?}",
        json
    );
}

fn create_writes_the_files_with_their_bits_and_prints_the_run_commands() {
    let shim = Shim::new(FLZ_BIN, TEMPLATE);
    let cwd = TempDir::new().expect("tempdir");

    let out = create(&shim, cwd.path(), &["java", "my-app"]);

    assert!(out.status.success(), "{:?}", out);
    let app = cwd.path().join("my-app");
    assert_eq!(
        fs::read_to_string(app.join(".gitignore")).unwrap(),
        "/target/\n"
    );
    assert_eq!(fs::read_to_string(app.join("src/a.txt")).unwrap(), "a\n");
    assert_ne!(
        mode(&app.join("bin/run")) & 0o100,
        0,
        "the manifest's executable bit"
    );
    assert_eq!(mode(&app.join("src/a.txt")) & 0o111, 0);

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("my-app"), "{stdout}");
    assert!(stdout.contains("    make test\n    make run\n"), "{stdout}");
    assert!(
        !stdout.contains("boot log"),
        "a boot line is not the answer"
    );
    assert!(String::from_utf8_lossy(&out.stderr).contains("a boot log line"));
}

fn create_sends_the_language_and_the_example() {
    let shim = Shim::new(
        FLZ_BIN,
        &TEMPLATE.replace("echo 'a boot", "echo \"$2\" >&2\necho 'a boot"),
    );
    let cwd = TempDir::new().expect("tempdir");

    let out = create(
        &shim,
        cwd.path(),
        &["rust", "my-app", "--example", "starter"],
    );

    assert!(
        String::from_utf8_lossy(&out.stderr)
            .contains(r#"Flezha.Mcp.Knowledge.Command.eval("create", ["rust", "starter"], [])"#),
        "{:?}",
        out
    );
}

fn a_folder_with_files_is_refused_and_nothing_is_written() {
    let shim = Shim::new(FLZ_BIN, TEMPLATE);
    let cwd = TempDir::new().expect("tempdir");
    fs::create_dir(cwd.path().join("my-app")).unwrap();
    fs::write(cwd.path().join("my-app/notes"), "mine").unwrap();

    let out = create(&shim, cwd.path(), &["java", "my-app"]);

    assert_eq!(out.status.code(), Some(6), "{:?}", out);
    assert!(String::from_utf8_lossy(&out.stderr).contains("(template.dir_not_empty)"));
    let left: Vec<_> = fs::read_dir(cwd.path().join("my-app")).unwrap().collect();
    assert_eq!(left.len(), 1);
}

fn an_empty_folder_is_filled() {
    let shim = Shim::new(FLZ_BIN, TEMPLATE);
    let cwd = TempDir::new().expect("tempdir");
    fs::create_dir(cwd.path().join("my-app")).unwrap();

    let out = create(&shim, cwd.path(), &["java", "my-app"]);

    assert!(out.status.success(), "{:?}", out);
    assert!(cwd.path().join("my-app/src/a.txt").exists());
}

fn an_unknown_template_is_the_release_s_refusal_and_creates_no_folder() {
    let shim = Shim::new(FLZ_BIN, UNKNOWN);
    let cwd = TempDir::new().expect("tempdir");

    let out = create(&shim, cwd.path(), &["cobol", "my-app"]);

    assert_eq!(out.status.code(), Some(5));
    assert!(String::from_utf8_lossy(&out.stderr).contains("(template.unknown)"));
    assert!(!cwd.path().join("my-app").exists());
}

fn tar_writes_the_same_files_to_stdout_and_touches_no_folder() {
    let shim = Shim::new(FLZ_BIN, TEMPLATE);
    let cwd = TempDir::new().expect("tempdir");

    let out = create(&shim, cwd.path(), &["java", "my-app", "--tar"]);

    assert!(out.status.success(), "{:?}", out);
    assert!(!cwd.path().join("my-app").exists());
    let host = TempDir::new().expect("tempdir");
    fs::write(host.path().join("t.tar"), &out.stdout).unwrap();
    let untarred = Command::new("tar")
        .current_dir(host.path())
        .args(["xf", "t.tar"])
        .status()
        .expect("tar runs");
    assert!(untarred.success());
    let app = host.path().join("my-app");
    assert_eq!(
        fs::read_to_string(app.join(".gitignore")).unwrap(),
        "/target/\n"
    );
    assert_eq!(fs::read_to_string(app.join("src/a.txt")).unwrap(), "a\n");
    assert_ne!(mode(&app.join("bin/run")) & 0o100, 0);
}

fn tar_to_a_terminal_is_refused_naming_the_redirect() {
    let shim = Shim::new(FLZ_BIN, TEMPLATE);
    let cwd = TempDir::new().expect("tempdir");
    let flz = cwd.path().join("flz-under-script");
    fs::copy(shim.flzvm_path().parent().unwrap().join("flz"), &flz).unwrap();
    fs::copy(shim.flzvm_path(), cwd.path().join("flzvm")).unwrap();

    // `script` gives the command a terminal for its stdout.
    let out = Command::new("script")
        .current_dir(cwd.path())
        .args([
            "-qec",
            "./flz-under-script template create java my-app --tar",
            "/dev/null",
        ])
        .output()
        .expect("script runs");

    assert_eq!(out.status.code(), Some(2), "{:?}", out);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("(usage.tar_to_terminal)"), "{text}");
    assert!(text.contains("| tar x"), "{text}");
    assert!(!cwd.path().join("my-app").exists());
}

/// One `#[test]`, several phases: the fixture execs a *copy* of the binary, and a
/// copy written while a sibling thread forks is `ExecutableFileBusy` in the
/// child that inherited the write handle. One thread, no race.
#[test]
fn flz_template_lists_creates_and_refuses() {
    list_and_create_ask_the_vm_of_their_own_with_no_token();
    create_writes_the_files_with_their_bits_and_prints_the_run_commands();
    create_sends_the_language_and_the_example();
    a_folder_with_files_is_refused_and_nothing_is_written();
    an_empty_folder_is_filled();
    an_unknown_template_is_the_release_s_refusal_and_creates_no_folder();
    tar_writes_the_same_files_to_stdout_and_touches_no_folder();
    tar_to_a_terminal_is_refused_naming_the_redirect();
}
