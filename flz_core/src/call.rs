//! The outcome of a call as it reaches the caller: the JSON envelope the
//! controller appends on the caller's Response channel, read back into its
//! bytes. A typed SDK decodes the request and the answer from here with its
//! own codecs.

use flz_transport::errors::FlezhaError;

/// One call's outcome with its request and answer still bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub request: Vec<u8>,
    pub result: Result<Vec<u8>, FlezhaError>,
    pub target: String,
    pub created: bool,
}

#[derive(serde::Deserialize)]
struct Wire {
    target: String,
    created: bool,
    request: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

impl Envelope {
    /// Read one Response channel payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let wire: Wire =
            serde_json::from_slice(bytes).map_err(|e| format!("a Response envelope: {e}"))?;
        let result = match (wire.value, wire.error) {
            (_, Some(error)) => Err(FlezhaError::from_value(&error)
                .ok_or("a Response error that is not an error document")?),
            (Some(value), None) => Ok(base64_decode(&value)?),
            (None, None) => return Err("a Response with neither a value nor an error".into()),
        };
        Ok(Self {
            request: base64_decode(&wire.request)?,
            result,
            target: wire.target,
            created: wire.created,
        })
    }
}

/// Standard padded base64, the alphabet the controller writes.
fn base64_decode(text: &str) -> Result<Vec<u8>, String> {
    let refuse = || format!("a Response field that is not base64: {text:?}");
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(refuse());
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for quad in bytes.chunks(4) {
        let mut acc: u32 = 0;
        let mut pad = 0;
        for &c in quad {
            let sextet = match c {
                b'A'..=b'Z' => c - b'A',
                b'a'..=b'z' => c - b'a' + 26,
                b'0'..=b'9' => c - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => {
                    pad += 1;
                    0
                }
                _ => return Err(refuse()),
            };
            if pad > 0 && c != b'=' {
                return Err(refuse());
            }
            acc = acc << 6 | u32::from(sextet);
        }
        let [_, a, b, c] = acc.to_be_bytes();
        out.extend_from_slice(&[a, b, c][..3 - pad.min(2)]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every case of the shared fixture reads back to the bytes it names.
    #[test]
    fn every_envelope_case_decodes() {
        let fixture: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string("../test/fixtures/call_response_envelope.json")
                .expect("the shared fixture"),
        )
        .expect("JSON");
        for case in fixture["cases"].as_array().expect("cases") {
            let (name, expect) = (&case["name"], &case["expect"]);
            let envelope = Envelope::decode(case["payload"].as_str().expect("payload").as_bytes())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let b64 = |field: &str| base64_decode(expect[field].as_str().expect(field)).unwrap();
            assert_eq!(envelope.request, b64("request"), "{name}");
            assert_eq!(envelope.target, expect["target"], "{name}");
            assert_eq!(envelope.created, expect["created"], "{name}");
            match envelope.result {
                Ok(value) => {
                    assert_eq!(expect["ok"], true, "{name}");
                    assert_eq!(value, b64("value"), "{name}");
                }
                Err(error) => {
                    assert_eq!(expect["ok"], false, "{name}");
                    assert_eq!(error.code, expect["error_code"], "{name}");
                    assert_eq!(error.class.as_str(), expect["error_class"], "{name}");
                    assert_eq!(error.message, expect["error_message"], "{name}");
                }
            }
        }
    }

    #[test]
    fn base64_of_every_length_reads_back() {
        for (text, bytes) in [
            ("", &b""[..]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("AP8QgA==", &[0, 255, 16, 128]),
        ] {
            assert_eq!(base64_decode(text).as_deref(), Ok(bytes), "{text}");
        }
        for bad in ["Zg=", "Z===x", "Zm9*", "=Zm9"] {
            assert!(base64_decode(bad).is_err(), "{bad}");
        }
    }
}
