//! `flz_core_knob`: every case of `test/fixtures/knob_parse_cases.txt` through
//! the C entry, which is the reader every hosted SDK parses its knobs with.

use std::ffi::CString;

use flz_core_ffi::{flz_core_knob, FLZ_ERR, FLZ_KNOB_UNSET, FLZ_OK};

fn knob(kind: &str, raw: &str) -> (i32, i64) {
    let (kind, raw) = (CString::new(kind).unwrap(), CString::new(raw).unwrap());
    let mut out = -7;
    let status = unsafe { flz_core_knob(kind.as_ptr(), raw.as_ptr(), &mut out) };
    (status, out)
}

#[test]
fn every_case_of_the_shared_fixture_parses_through_the_entry() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../test/fixtures/knob_parse_cases.txt"
    );
    let fixture = std::fs::read_to_string(path).expect("the shared parse fixture");
    let cases = fixture
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty());
    for case in cases {
        let [kind, input, expected] = case.split('|').collect::<Vec<_>>()[..] else {
            panic!("a case is type|input|expected: {case}");
        };
        let input = if input == "<empty>" { "" } else { input };
        let kind = if kind == "positive_int" {
            "positive"
        } else {
            kind
        };
        let got = match knob(kind, input) {
            (FLZ_OK, value) if kind == "bool" => (value == 1).to_string(),
            (FLZ_OK, value) => value.to_string(),
            (FLZ_KNOB_UNSET, _) => "error".to_string(),
            other => panic!("{case} answered {other:?}"),
        };
        assert_eq!(got, expected, "{case}");
    }
}

#[test]
fn a_kind_the_core_has_no_rule_for_is_refused() {
    assert_eq!(knob("percent", "5").0, FLZ_ERR);
    let mut out = 0;
    assert_eq!(
        unsafe { flz_core_knob(std::ptr::null(), std::ptr::null(), &mut out) },
        FLZ_ERR
    );
}
