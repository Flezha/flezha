//! The error model's Rust half: the catalogue `errors.json` beside this
//! crate's manifest, the one error type every Rust surface reports a failure
//! as, and its two renderings. The controller renders the same catalogue in
//! `Flezha.Error`; `test/fixtures/error_rendering.json` holds both renderers to
//! the same bytes.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

use serde_json::{Map, Value};

const CATALOGUE: &str = include_str!("../errors.json");

/// The code a failure falls back to when its own is not in the catalogue.
pub const UNCATALOGUED: &str = "internal.uncatalogued";

/// What kind of failure a code is, and so what a caller can do about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    Internal,
    Invalid,
    Unreachable,
    Denied,
    NotFound,
    Conflict,
    Unavailable,
}

impl Class {
    pub const ALL: [Class; 7] = [
        Class::Internal,
        Class::Invalid,
        Class::Unreachable,
        Class::Denied,
        Class::NotFound,
        Class::Conflict,
        Class::Unavailable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Class::Internal => "internal",
            Class::Invalid => "invalid",
            Class::Unreachable => "unreachable",
            Class::Denied => "denied",
            Class::NotFound => "not_found",
            Class::Conflict => "conflict",
            Class::Unavailable => "unavailable",
        }
    }

    pub fn parse(word: &str) -> Option<Class> {
        Class::ALL.into_iter().find(|class| class.as_str() == word)
    }

    /// The process exit code the catalogue assigns this class.
    pub fn exit_code(self) -> i32 {
        catalogue()
            .classes
            .iter()
            .find(|(class, _)| *class == self)
            .map_or(1, |(_, exit)| *exit)
    }
}

impl fmt::Display for Class {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One catalogued code: its class, its message and hint templates, and the
/// detail keys the templates may name.
#[derive(Debug)]
pub struct Entry {
    pub class: Class,
    pub message: String,
    pub hint: Option<String>,
    pub details: Vec<String>,
}

#[derive(Debug)]
struct Catalogue {
    classes: Vec<(Class, i32)>,
    codes: BTreeMap<String, Entry>,
}

fn catalogue() -> &'static Catalogue {
    static PARSED: OnceLock<Catalogue> = OnceLock::new();
    PARSED.get_or_init(|| parse(CATALOGUE))
}

// The file is part of this crate's source, so a malformed one is a build
// defect the first test run names, not a runtime condition.
fn parse(text: &str) -> Catalogue {
    let root: Value = serde_json::from_str(text).expect("errors.json is valid JSON");

    let classes = root["classes"]
        .as_object()
        .expect("errors.json has a classes object")
        .iter()
        .map(|(word, exit)| {
            let class = Class::parse(word).expect("errors.json names only the seven classes");
            (
                class,
                exit.as_i64().expect("an exit code is a number") as i32,
            )
        })
        .collect();

    let codes = root["codes"]
        .as_object()
        .expect("errors.json has a codes object")
        .iter()
        .map(|(code, entry)| {
            let text = |key: &str| entry.get(key).and_then(Value::as_str).map(str::to_string);
            let entry = Entry {
                class: Class::parse(entry["class"].as_str().unwrap_or_default())
                    .unwrap_or_else(|| panic!("{code} names an unknown class")),
                message: text("message").unwrap_or_else(|| panic!("{code} has no message")),
                hint: text("hint"),
                details: entry
                    .get("details")
                    .and_then(Value::as_array)
                    .map(|keys| {
                        keys.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_string)
                            .collect()
                    })
                    .unwrap_or_default(),
            };
            (code.clone(), entry)
        })
        .collect();

    Catalogue { classes, codes }
}

/// The catalogue entry for `code`, if there is one.
pub fn entry(code: &str) -> Option<&'static Entry> {
    catalogue().codes.get(code)
}

/// Every catalogued code, in order.
pub fn codes() -> impl Iterator<Item = &'static str> {
    catalogue().codes.keys().map(String::as_str)
}

/// A failure in the error model's one shape.
#[derive(Debug, Clone, PartialEq)]
pub struct FlezhaError {
    pub code: String,
    pub class: Class,
    pub message: String,
    pub details: Map<String, Value>,
    pub hint: Option<String>,
}

/// The error model's fields on every error type a caller can receive: the
/// catalogue `code` a program branches on, its `class`, the `details` its
/// message names, and the `hint` naming the next command, when there is one.
/// An implementor says only how it renders into the one shape.
pub trait ErrorModel {
    /// The error in the model's one shape.
    fn to_error(&self) -> FlezhaError;

    fn code(&self) -> String {
        self.to_error().code
    }

    fn class(&self) -> Class {
        self.to_error().class
    }

    fn details(&self) -> Map<String, Value> {
        self.to_error().details
    }

    fn hint(&self) -> Option<String> {
        self.to_error().hint
    }
}

impl FlezhaError {
    /// The error `code` names, its templates filled from `details`. A code the
    /// catalogue does not know becomes `internal.uncatalogued`, carrying the
    /// code it was asked for, so a missed entry is still a readable failure.
    pub fn new(code: &str, details: Map<String, Value>) -> Self {
        match entry(code) {
            Some(entry) => FlezhaError {
                code: code.to_string(),
                class: entry.class,
                message: render_template(&entry.message, &details),
                hint: entry
                    .hint
                    .as_deref()
                    .map(|hint| render_template(hint, &details)),
                details,
            },
            None => {
                let mut details = details;
                details.insert("reason".into(), Value::String(code.to_string()));
                FlezhaError::new(UNCATALOGUED, details)
            }
        }
    }

    /// `new` for a code whose details are written inline as pairs.
    pub fn with<const N: usize>(code: &str, pairs: [(&str, Value); N]) -> Self {
        FlezhaError::new(
            code,
            pairs
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        )
    }

    /// The error a JSON error document carries, as the controller and the core
    /// render it. `None` when `document` is not one.
    pub fn from_document(document: &str) -> Option<Self> {
        let root: Value = serde_json::from_str(document).ok()?;
        FlezhaError::from_value(root.get("error")?)
    }

    /// The error one `{code, class, message, details, hint}` object carries.
    pub fn from_value(error: &Value) -> Option<Self> {
        Some(FlezhaError {
            code: error.get("code")?.as_str()?.to_string(),
            class: Class::parse(error.get("class")?.as_str()?)?,
            message: error.get("message")?.as_str()?.to_string(),
            details: error
                .get("details")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            hint: error
                .get("hint")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    pub fn exit_code(&self) -> i32 {
        self.class.exit_code()
    }

    /// `error: <message> (<code>)`, then `hint: <hint>` when there is one.
    pub fn text(&self) -> String {
        match &self.hint {
            Some(hint) => format!("error: {} ({})\nhint: {}", self.message, self.code, hint),
            None => format!("error: {} ({})", self.message, self.code),
        }
    }

    /// The one-line JSON error document. Written field by field so the key
    /// order is fixed, with `details` sorted by key at every depth.
    pub fn json(&self) -> String {
        format!(
            "{{\"error\":{{\"code\":{},\"class\":{},\"message\":{},\"details\":{},\"hint\":{}}}}}",
            quoted(&self.code),
            quoted(self.class.as_str()),
            quoted(&self.message),
            self.details_json(),
            self.hint.as_deref().map_or("null".to_string(), quoted),
        )
    }

    /// `details` as the compact, key-sorted JSON object the wire carries.
    pub fn details_json(&self) -> String {
        let mut out = String::new();
        write_sorted(&Value::Object(self.details.clone()), &mut out);
        out
    }

    /// The rendering a caller asked for: `json` or anything else as text.
    pub fn render(&self, format: &str) -> String {
        if format == "json" {
            self.json()
        } else {
            self.text()
        }
    }
}

impl fmt::Display for FlezhaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FlezhaError {}

fn quoted(text: &str) -> String {
    serde_json::to_string(text).expect("a string encodes")
}

// Compact JSON with every object's keys in byte order, whatever map type the
// value was built in: the two renderers agree only if neither inherits an
// order from its map.
fn write_sorted(value: &Value, out: &mut String) {
    match value {
        Value::Object(object) => {
            let keys: BTreeMap<&String, &Value> = object.iter().collect();
            out.push('{');
            for (i, (key, value)) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&quoted(key));
                out.push(':');
                write_sorted(value, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_sorted(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// `template` with every `{key}` replaced by `details[key]` as text: a string
/// as written, a list of strings joined by `, `, anything else as compact JSON.
/// A key the details do not carry is left as written.
pub fn render_template(template: &str, details: &Map<String, Value>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after.find('}') {
            Some(close) if is_key(&after[..close]) => {
                let key = &after[..close];
                match details.get(key) {
                    Some(value) => out.push_str(&as_text(value)),
                    None => out.push_str(&rest[open..open + close + 2]),
                }
                rest = &after[close + 1..];
            }
            _ => {
                out.push('{');
                rest = after;
            }
        }
    }

    out.push_str(rest);
    out
}

fn is_key(word: &str) -> bool {
    !word.is_empty()
        && word
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn as_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) if items.iter().all(Value::is_string) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(", "),
        other => {
            let mut out = String::new();
            write_sorted(other, &mut out);
            out
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn details(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn a_code_renders_its_message_code_and_hint() {
        let error = FlezhaError::new(
            "namespace.not_found",
            details(json!({"namespace": "ghost"})),
        );

        assert_eq!(error.class, Class::NotFound);
        assert_eq!(
            error.text(),
            "error: namespace 'ghost' does not exist (namespace.not_found)\nhint: flz namespace list"
        );
        assert_eq!(
            error.json(),
            r#"{"error":{"code":"namespace.not_found","class":"not_found","message":"namespace 'ghost' does not exist","details":{"namespace":"ghost"},"hint":"flz namespace list"}}"#
        );
    }

    #[test]
    fn a_missing_placeholder_is_left_as_written() {
        let error = FlezhaError::new("namespace.not_found", Map::new());

        assert_eq!(error.message, "namespace '{namespace}' does not exist");
    }

    #[test]
    fn a_code_outside_the_catalogue_is_uncatalogued_and_names_itself() {
        let error = FlezhaError::new("nope.never", Map::new());

        assert_eq!(error.code, UNCATALOGUED);
        assert_eq!(error.class, Class::Internal);
        assert_eq!(error.details["reason"], json!("nope.never"));
    }

    #[test]
    fn every_class_has_its_exit_code() {
        let exits: Vec<(Class, i32)> = Class::ALL.iter().map(|c| (*c, c.exit_code())).collect();

        assert_eq!(
            exits,
            vec![
                (Class::Internal, 1),
                (Class::Invalid, 2),
                (Class::Unreachable, 3),
                (Class::Denied, 4),
                (Class::NotFound, 5),
                (Class::Conflict, 6),
                (Class::Unavailable, 7),
            ]
        );
    }

    #[test]
    fn a_document_reads_back_as_the_error_it_renders() {
        let error = FlezhaError::new(
            "quota.exceeded",
            details(json!({"kind": "max_relays", "current": 3, "limit": 3})),
        );

        assert_eq!(FlezhaError::from_document(&error.json()), Some(error));
        assert_eq!(FlezhaError::from_document("not json"), None);
    }

    #[test]
    fn every_catalogued_code_is_spelled_family_dot_snake_and_its_templates_name_declared_keys() {
        for code in codes() {
            let (family, name) = code.split_once('.').expect("a code has a family");
            assert!(is_key(family) && is_key(name), "{code} is not family.snake");

            let entry = entry(code).unwrap();
            for template in std::iter::once(&entry.message).chain(entry.hint.iter()) {
                for key in template
                    .split('{')
                    .skip(1)
                    .filter_map(|part| part.split_once('}'))
                    .map(|(key, _)| key)
                    .filter(|key| is_key(key))
                {
                    assert!(
                        entry.details.iter().any(|declared| declared == key),
                        "{code} names {{{key}}} but declares no such detail"
                    );
                }
            }
        }
    }

    /// The shared fixture both renderers answer to.
    #[test]
    fn the_shared_rendering_fixture_renders_byte_for_byte() {
        let fixture: Value =
            serde_json::from_str(include_str!("../../test/fixtures/error_rendering.json"))
                .expect("the fixture is JSON");

        for case in fixture.as_array().expect("the fixture is a list") {
            let error = FlezhaError::new(
                case["code"].as_str().unwrap(),
                details(case["details"].clone()),
            );
            assert_eq!(error.text(), case["text"].as_str().unwrap(), "{case}");
            assert_eq!(error.json(), case["json"].as_str().unwrap(), "{case}");
        }
    }
}
