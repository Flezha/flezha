use tempfile::TempDir;

use super::*;

fn rung(dir: &TempDir, rung: Rung) -> (Rung, PathBuf) {
    (rung, dir.path().to_path_buf())
}

#[test]
fn each_rung_answers_when_it_is_the_only_live_one() {
    let xdg = TempDir::new().unwrap();
    let run = TempDir::new().unwrap();
    let repo = TempDir::new().unwrap();

    for (expected, holder) in [
        (Rung::XdgRuntime, &xdg),
        (Rung::Run, &run),
        (Rung::Repository, &repo),
    ] {
        let _live = listen(holder.path());
        let resolved = resolve_from(
            vec![
                rung(&xdg, Rung::XdgRuntime),
                rung(&run, Rung::Run),
                rung(&repo, Rung::Repository),
            ],
            Role::Runner,
        )
        .expect("one live candidate resolves");

        assert_eq!(resolved.rung, expected);
        assert_eq!(resolved.path, holder.path());
        assert_eq!(resolved.walked.len(), 3);
        drop(_live);
        let _ = fs::remove_file(path_in(holder.path(), CONTROL_SOCKET_FILE));
    }
}

#[test]
fn an_explicit_path_never_falls_back_to_a_live_candidate() {
    let explicit = TempDir::new().unwrap();
    let live = TempDir::new().unwrap();
    let _listener = listen(live.path());

    let resolved = resolve_from(
        candidates_from(Some(explicit.path().to_path_buf()), None),
        Role::Runner,
    )
    .expect("explicit resolves");

    assert_eq!(resolved.rung, Rung::Explicit);
    assert_eq!(resolved.path, explicit.path());
    // The diagnosis names the configured path only — never the ladder it
    // did not walk.
    assert_eq!(resolved.walked, vec![explicit.path().to_path_buf()]);
}

#[test]
fn two_live_candidates_are_refused_and_both_are_named() {
    let xdg = TempDir::new().unwrap();
    let run = TempDir::new().unwrap();
    let _first = listen(xdg.path());
    let _second = listen(run.path());

    let refusal = resolve_from(
        vec![rung(&xdg, Rung::XdgRuntime), rung(&run, Rung::Run)],
        Role::Runner,
    )
    .expect_err("two controllers are an ambiguity, not a choice");

    match &refusal {
        SocketDirError::Ambiguous { live } => {
            assert_eq!(
                live,
                &vec![xdg.path().to_path_buf(), run.path().to_path_buf()]
            );
        }
        other => panic!("expected an ambiguity refusal, got {:?}", other),
    }
    // The refusal names the variable that resolves it.
    assert!(refusal.to_string().contains(SOCKET_DIR_VAR));
}

/// The env contract is data both sides read, exactly as the declaration
/// keys are: `test/fixtures/runner_connect_env.txt`. Here the ladder's own
/// constants and rung order are asserted against it; the static gate
/// asserts the same file against every other SDK.
#[test]
fn the_pinned_environment_contract_is_what_the_ladder_reads() {
    const FIXTURE: &str = include_str!("../../../test/fixtures/runner_connect_env.txt");

    let rows: Vec<Vec<&str>> = FIXTURE
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.split_whitespace().collect())
        .collect();

    let variables: Vec<&str> = rows
        .iter()
        .filter(|row| row[0] == "variable")
        .map(|row| row[1])
        .collect();
    assert!(variables.contains(&SOCKET_DIR_VAR));
    assert!(variables.contains(&XDG_RUNTIME_VAR));
    assert!(
        variables.contains(&crate::transport::config::QUIC_ADDRESSES_VAR),
        "the QUIC list variable is part of the same contract"
    );

    let retired: Vec<&str> = rows
        .iter()
        .filter(|row| row[0] == "retired")
        .map(|row| row[1])
        .collect();
    assert_eq!(
        retired,
        vec![crate::transport::config::RETIRED_QUIC_ADDRESS_VAR]
    );

    let ladder: Vec<(&str, &str)> = rows
        .iter()
        .filter(|row| row[0] == "ladder")
        .map(|row| (row[2], row[3]))
        .collect();
    assert_eq!(
        ladder,
        vec![
            ("explicit", "$FLEZHA_SOCKET_FILES_PATH"),
            ("xdg_runtime", "$XDG_RUNTIME_DIR/flezha"),
            ("run", RUN_CANDIDATE),
            ("repository", REPOSITORY_CANDIDATE),
        ],
        "the ladder and test/fixtures/runner_connect_env.txt disagree — \
         teach the fixture, then every SDK"
    );

    // The rungs the ladder actually offers, in the order it offers them.
    assert_eq!(
        candidates_from(None, Some(PathBuf::from("/tmp")))
            .iter()
            .map(|(rung, _)| rung.label())
            .collect::<Vec<_>>(),
        vec![ladder[1].1, ladder[2].1, ladder[3].1]
    );
    assert_eq!(
        candidates_from(Some(PathBuf::from("/tmp")), None)[0]
            .0
            .label(),
        ladder[0].1
    );
}

#[test]
fn an_unset_runtime_dir_falls_through_to_the_later_rungs() {
    let ladder = candidates_from(None, None);

    assert_eq!(
        ladder.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![Rung::Run, Rung::Repository]
    );

    let with_runtime = candidates_from(None, Some(PathBuf::from("/tmp/runtime-42")));
    assert_eq!(
        with_runtime.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![Rung::XdgRuntime, Rung::Run, Rung::Repository]
    );
    assert!(with_runtime[0].1.ends_with("flezha"));
}

#[test]
fn a_relative_candidate_is_reported_absolutely() {
    let ladder = candidates_from(None, None);
    let (_, repository) = ladder
        .last()
        .expect("the repository rung is always offered");

    assert!(repository.is_absolute(), "{}", repository.display());
    assert!(repository.ends_with(REPOSITORY_CANDIDATE));

    let explicit = candidates_from(Some(PathBuf::from("relative/sockets")), None);
    assert!(explicit[0].1.is_absolute());
}

#[test]
fn nothing_live_reports_the_candidate_with_the_most_to_say() {
    let empty = TempDir::new().unwrap();
    let holder = TempDir::new().unwrap();
    // A socket file nobody listens on: a controller that died hard.
    fs::write(path_in(holder.path(), CONTROL_SOCKET_FILE), b"").unwrap();

    let resolved = resolve_from(
        vec![rung(&empty, Rung::Run), rung(&holder, Rung::Repository)],
        Role::Runner,
    )
    .expect("a dead controller still resolves a path to report");

    assert_eq!(resolved.path, holder.path());
}

#[test]
fn the_controller_takes_the_first_candidate_it_can_create() {
    let root = TempDir::new().unwrap();
    let unwritable = PathBuf::from("/proc/flezha-cannot-create-this");
    let wanted = root.path().join("sockets");

    let resolved = resolve_from(
        vec![
            (Rung::XdgRuntime, unwritable),
            (Rung::Run, wanted.clone()),
            (Rung::Repository, root.path().join("fallback")),
        ],
        Role::Controller,
    )
    .expect("a creatable candidate resolves");

    assert_eq!(resolved.rung, Rung::Run);
    assert!(wanted.is_dir());
}

#[test]
fn a_controller_steps_over_a_directory_another_controller_is_listening_in() {
    let taken = TempDir::new().unwrap();
    let free = TempDir::new().unwrap();
    let _incumbent = listen(taken.path());

    let resolved = resolve_from(
        vec![rung(&taken, Rung::XdgRuntime), rung(&free, Rung::Run)],
        Role::Controller,
    )
    .expect("the next candidate is creatable");

    assert_eq!(
        resolved.path,
        free.path(),
        "taking the occupied directory would wipe a live controller's sockets"
    );
}

#[test]
fn a_recorded_directory_settles_an_ambiguity_a_first_connect_would_refuse() {
    let first = TempDir::new().unwrap();
    let second = TempDir::new().unwrap();
    let _a = listen(first.path());
    let _b = listen(second.path());

    note_connected(&path_in(second.path(), CONTROL_SOCKET_FILE));

    let resolved = resolve_from(
        vec![rung(&first, Rung::XdgRuntime), rung(&second, Rung::Run)],
        Role::Runner,
    )
    .expect("a reconnect returns to the controller holding the bindings");

    assert_eq!(resolved.path, second.path());

    // Leave the process-wide slot as the rest of the binary expects it.
    LAST_GOOD_DIR.forget();
}

// A 48-byte socket directory, the most the Elixir test harness mints under
// `/tmp`, and a runner id at the width a late-scheduled test module draws.
const PEER_DIR: &str = "/tmp/flezha_sockets/native_reject_peer@127.0.0.1";
const LONG_ID: &str = "schema_evolution_e2e_419362_660179359383";

#[test]
fn a_long_runner_id_composes_a_data_socket_a_peer_directory_can_bind() {
    let dir = PathBuf::from(PEER_DIR);
    fs::create_dir_all(&dir).expect("the peer directory");

    let path = stream_socket_path_in(&dir, LONG_ID, "secondary").expect("the name fits the wall");

    assert!(
        path.len() <= 107,
        "{} is {} bytes; sun_path holds 107 and a NUL",
        path,
        path.len()
    );

    let _ = fs::remove_file(&path);
    std::os::unix::net::UnixListener::bind(&path)
        .unwrap_or_else(|e| panic!("the controller binds {}: {}", path, e));
    let _ = fs::remove_file(&path);
}

#[test]
fn the_id_field_is_shortened_by_the_id_alone_and_never_by_the_directory() {
    let short = PathBuf::from("/run/flezha");
    let long = PathBuf::from(PEER_DIR);

    let in_short = stream_socket_path_in(&short, LONG_ID, "secondary").expect("fits");
    let in_long = stream_socket_path_in(&long, LONG_ID, "secondary").expect("fits");

    // The controller and the runner resolve their directories separately;
    // two file names for one runner would never meet.
    assert_eq!(
        Path::new(&in_short).file_name(),
        Path::new(&in_long).file_name()
    );
}

#[test]
fn a_shortened_id_is_the_ids_own_prefix_and_its_digest() {
    let field = steps_id(LONG_ID);

    assert_eq!(field.len(), STREAM_SOCKET_ID_MAX);
    // The digest is SHA-256 over the runner id's bytes, so it is
    // reproducible outside this repository: `printf %s <id> | sha256sum`.
    assert_eq!(field, "schema_evol~d33017db4b61");
    assert_eq!(steps_id(LONG_ID), field, "the same id gives the same field");

    // The literal every other end of this name is pinned to;
    // `Flezha.Socket.SupportTest` asserts the same string from the
    // controller's side.
    assert_eq!(
        stream_socket_path_in(Path::new(PEER_DIR), LONG_ID, "secondary").expect("fits"),
        format!(
            "{}/secondary_stream_schema_evol~d33017db4b61.sock",
            PEER_DIR
        )
    );
}

#[test]
fn an_id_within_the_ceiling_stays_readable() {
    // The longest id `Identificator.unique_id/1` mints in the default
    // namespace: four numbers of four digits each.
    let id = "default_1000100010001000";
    assert_eq!(id.len(), STREAM_SOCKET_ID_MAX);
    assert_eq!(steps_id(id), id);
    assert_eq!(steps_id("default_1_2_3_4"), "default_1_2_3_4");
}

#[test]
fn a_directory_that_cannot_hold_a_data_socket_is_refused_by_name_and_length() {
    let dir = PathBuf::from(format!("/tmp/{}", "d".repeat(80)));

    let refusal = stream_socket_path_in(&dir, "default_1_2_3_4", "secondary")
        .expect_err("a directory this long leaves no room for a socket name");

    let text = refusal.to_string();
    assert!(
        text.contains("secondary_stream_default_1_2_3_4.sock"),
        "{}",
        text
    );
    assert!(text.contains("123 bytes"), "{}", text);
    assert!(text.contains("107"), "{}", text);
}

#[test]
fn the_socket_predicate_recognises_the_names_this_module_creates() {
    assert!(is_flezha_socket(CONTROL_SOCKET_FILE));
    for stream_type in ["primary", "secondary"] {
        for id in ["runner-1", &steps_id(LONG_ID)] {
            let name = format!(
                "{}{}{}{}",
                stream_type, STREAM_SOCKET_INFIX, id, SOCKET_SUFFIX
            );
            assert!(is_flezha_socket(&name), "{}", name);
        }
    }

    // A directory a user named through FLEZHA_SOCKET_FILES_PATH may
    // hold anything else, and none of it is ours.
    for theirs in [
        "user-notes.txt",
        "postgres.sock",
        "runner_control_channel.sock.bak",
        "primary_stream_runner-1.sock.lock",
    ] {
        assert!(!is_flezha_socket(theirs), "{}", theirs);
    }
}
