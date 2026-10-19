//! The administration surface costs the shared bindings one entry, once.
//!
//! The claim the change makes is that adding a verb is a wrapper in each
//! host's own language and no native work at all. That is only true while the
//! verb travels as data, so this file holds the two halves of it: the C ABI
//! carries exactly one administration entry, and no verb name is written
//! anywhere in the shared bindings.

use std::fs;
use std::path::Path;

use crate::surface::exported_entry_points;

#[test]
fn the_c_abi_carries_exactly_one_administration_entry() {
    let admin: Vec<String> = exported_entry_points()
        .into_iter()
        .filter(|name| name.contains("admin"))
        .collect();

    assert_eq!(
        admin,
        vec!["flz_core_admin_call".to_string()],
        "the administration surface is verb-keyed: one entry for every verb, so a further \
         verb is a wrapper in each host's language and no ABI change"
    );
}

#[test]
fn no_verb_name_is_written_in_the_shared_bindings() {
    // A verb reaches the controller as data. If one were spelled here, adding
    // the next would mean touching this crate and every glue in it, which is
    // the cost the one entry exists to remove.
    let sources = ["src/abi/admin.rs", "src/veneer/mod.rs"];

    for relative in sources {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
        let source = fs::read_to_string(&path).expect("a source of this crate is readable");

        // Documentation names a verb as an example; code must not.
        let code: String = source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        for spelled in ["Relay.start", "Relay.stop", "PluginManager.enable"] {
            assert!(
                !code.contains(spelled),
                "{relative} spells the verb `{spelled}`; the shared bindings must carry no \
                 verb name, or every added verb costs a native change"
            );
        }
    }
}
