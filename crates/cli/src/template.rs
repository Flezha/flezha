//! `flz template`: the templates the installed release carries. Both verbs
//! read them in a VM of their own (`flzvm eval`), the reader `flz mcp`'s
//! `flezha_new_project` answers from, so no controller is reached. `create`
//! then writes the files itself: into a new folder, or as a tar stream on
//! stdout for a release inside a container to hand to the host.

use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, IsTerminal, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path};
use std::time::{SystemTime, UNIX_EPOCH};

use flz_transport::errors::FlezhaError;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::args::{OptSpec, Positional, Reading};
use crate::dispatch::{Command, Missing, Verb};
use crate::help;
use crate::rpc::{self, text, Out};

/// Where both verbs run, in a VM of its own.
const LOCAL: &str = "Flezha.Mcp.Knowledge.Command";

const EXAMPLE: OptSpec =
    OptSpec::text("--example", "--example", "<name>").doc("The template (default: starter).");
const TAR: OptSpec = OptSpec::switch("--tar", "--tar")
    .doc("Write the files as a tar stream on stdout instead of a folder.");

pub(crate) const TABLE: Command = Command {
    path: "flz template",
    title: "Start a project from a template the release carries.",
    notes: Some(help::TEMPLATE),
    verbs: &[
        Verb::new("list", |read| {
            rpc::eval(LOCAL, "eval", &[text("list"), "[]".into()], read)
        })
        .about("Print each language and example the release carries."),
        Verb::new("create", handle_create)
            .taking(&[
                Positional::required("<language>"),
                Positional::required("<dir>"),
            ])
            .reading(&[EXAMPLE, TAR])
            .strict()
            .about("Write a template's files into a new folder <dir>."),
    ],
    missing: Missing::Refuse,
    unknown_prints_help: false,
};

/// A template, as the release answers it.
#[derive(Deserialize)]
struct Template {
    files: Vec<File>,
    run: Vec<String>,
}

#[derive(Deserialize)]
struct File {
    path: String,
    content: String,
    executable: bool,
}

fn handle_create(read: &Reading) {
    let dir = read.pos("<dir>");
    let tar = read.switch(TAR.long);

    if tar && io::stdout().is_terminal() {
        rpc::fail(FlezhaError::with("usage.tar_to_terminal", []));
    }
    if !tar && holds_files(Path::new(dir)) {
        rpc::fail(FlezhaError::with(
            "template.dir_not_empty",
            [("dir", Value::from(dir))],
        ));
    }

    let template = template(read);

    if tar {
        if let Err(e) = write_tar(dir, &template.files) {
            rpc::write_failed("stdout", e);
        }
    } else {
        write_dir(Path::new(dir), &template.files);
        created(read, dir, &template);
    }
}

fn holds_files(dir: &Path) -> bool {
    fs::read_dir(dir).is_ok_and(|mut entries| entries.next().is_some())
}

/// The template's one JSON line. Anything else the child prints, such as a
/// boot log line, goes to stderr.
fn template(read: &Reading) -> Template {
    let example = read.opt(EXAMPLE.long).unwrap_or("starter");
    let args = format!("[{}, {}]", text(read.pos("<language>")), text(example));
    let request = rpc::eval_request(LOCAL, "eval", &[text("create"), args], read);

    let mut answer = None;
    let mut keep = |line: &str| {
        if line.starts_with('{') {
            answer = serde_json::from_str::<Template>(line).ok();
        } else {
            eprintln!("{}", line);
        }
        false
    };
    rpc::settle(rpc::child("eval", &request, Out::Filter(&mut keep), None));

    let template = answer.unwrap_or_else(|| {
        rpc::fail(FlezhaError::with(
            "control.unreadable_answer",
            [("verb", Value::from("template create"))],
        ))
    });

    // Each path is joined to the folder, so only a plain relative one is taken.
    for file in &template.files {
        let mut parts = Path::new(&file.path).components().peekable();
        if parts.peek().is_none() || !parts.all(|part| matches!(part, Component::Normal(_))) {
            rpc::write_failed(&file.path, "not a path inside the folder");
        }
    }
    template
}

/// Each file created new, with the user's umask and the template's
/// executable bit.
fn write_dir(dir: &Path, files: &[File]) {
    for file in files {
        let path = dir.join(&file.path);
        let mode = if file.executable { 0o777 } else { 0o666 };
        let written = path
            .parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(mode)
                    .open(&path)
            })
            .and_then(|mut out| out.write_all(file.content.as_bytes()));

        if let Err(e) = written {
            rpc::write_failed(path.display(), e);
        }
    }
}

fn created(read: &Reading, dir: &str, template: &Template) {
    if read.output_format() == "json" {
        let paths: Vec<&str> = template
            .files
            .iter()
            .map(|file| file.path.as_str())
            .collect();
        println!(
            "{}",
            json!({"dir": dir, "files": paths, "run": template.run})
        );
    } else {
        println!(
            "Created {} with {} files. Next, in {}:",
            dir,
            template.files.len(),
            dir
        );
        for line in &template.run {
            println!("    {}", line);
        }
        println!("README.md explains each step.");
    }
}

// ---------- the tar stream ----------

const BLOCK: usize = 512;

/// A POSIX tar of `files` under `dir`: each folder first, then each file, so
/// `tar x` creates `dir` owned by whoever runs it.
fn write_tar(dir: &str, files: &[File]) -> io::Result<()> {
    let root = dir.trim_end_matches('/');
    let mtime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());

    let mut folders = BTreeSet::from([String::new()]);
    for file in files {
        for parent in Path::new(&file.path).ancestors().skip(1) {
            folders.insert(parent.display().to_string());
        }
    }

    let mut out = io::stdout().lock();
    for folder in folders {
        let name = if folder.is_empty() {
            format!("{}/", root)
        } else {
            format!("{}/{}/", root, folder)
        };
        out.write_all(&header(&name, 0o755, 0, mtime, b'5')?)?;
    }
    for file in files {
        let name = format!("{}/{}", root, file.path);
        let mode = if file.executable { 0o755 } else { 0o644 };
        let body = file.content.as_bytes();
        out.write_all(&header(&name, mode, body.len() as u64, mtime, b'0')?)?;
        out.write_all(body)?;
        out.write_all(&[0; BLOCK][..(BLOCK - body.len() % BLOCK) % BLOCK])?;
    }
    out.write_all(&[0; 2 * BLOCK])?;
    out.flush()
}

/// One ustar header: a name over 100 bytes is split into the prefix field at
/// a `/`.
fn header(name: &str, mode: u64, size: u64, mtime: u64, kind: u8) -> io::Result<[u8; BLOCK]> {
    let (prefix, name) = if name.len() <= 100 {
        ("", name)
    } else {
        name.char_indices()
            .filter(|&(at, c)| c == '/' && at <= 155 && name.len() - at - 1 <= 100)
            .map(|(at, _)| (&name[..at], &name[at + 1..]))
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path too long for tar"))?
    };

    let mut block = [0; BLOCK];
    block[..name.len()].copy_from_slice(name.as_bytes());
    octal(&mut block[100..108], mode);
    octal(&mut block[108..116], 0);
    octal(&mut block[116..124], 0);
    octal(&mut block[124..136], size);
    octal(&mut block[136..148], mtime);
    block[156] = kind;
    block[257..263].copy_from_slice(b"ustar\0");
    block[263..265].copy_from_slice(b"00");
    block[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());

    // The checksum is summed with its own field read as spaces.
    block[148..156].fill(b' ');
    let sum = block.iter().map(|&byte| u64::from(byte)).sum();
    octal(&mut block[148..155], sum);
    Ok(block)
}

/// `value` in octal, zero-padded to the field less its closing NUL.
fn octal(field: &mut [u8], value: u64) {
    let width = field.len() - 1;
    field[..width].copy_from_slice(format!("{:0width$o}", value, width = width).as_bytes());
    field[width] = 0;
}
