//! `FLEZHA_MAILBOX_CAPACITY` sizes a tasklet that declares no mailbox. A
//! binary of its own, because the variable is process-wide and every other
//! test that declares a class would read it.

fn capacity_of(tasklet: &str) -> usize {
    let json = format!(
        r#"{{"factory_id":"knob","sdk_lang":1,"sdk_version":"0.1.0",
             "channels":[{{"name":"in","kind":"entry","chains":[[{{"link":"into","tasklet":0}}]]}}],
             "tasklets":[{tasklet}]}}"#
    );
    let derived = flz_core::host::check_class_json(json.as_bytes()).expect("the class checks");
    derived.decl.tasklets[0].mailbox_capacity
}

#[test]
fn host_mailbox_capacity_defaults_to_the_knob() {
    std::env::remove_var("FLEZHA_MAILBOX_CAPACITY");
    assert_eq!(capacity_of(r#"{"name":"p"}"#), 1024);

    std::env::set_var("FLEZHA_MAILBOX_CAPACITY", "64");
    assert_eq!(capacity_of(r#"{"name":"p"}"#), 64);
    assert_eq!(
        capacity_of(r#"{"name":"p","mailbox_capacity":8}"#),
        8,
        "a declared capacity wins over the knob"
    );

    for invalid in ["0", "-3", "lots"] {
        std::env::set_var("FLEZHA_MAILBOX_CAPACITY", invalid);
        assert_eq!(capacity_of(r#"{"name":"p"}"#), 1024, "{invalid}");
    }
}
