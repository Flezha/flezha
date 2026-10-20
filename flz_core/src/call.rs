//! The outcome of a call as it reaches the caller: the JSON envelope the
//! controller appends on the caller's Response channel, read back into its
//! bytes. A typed SDK decodes the request and the answer from here with its
//! own codecs.

use flz_transport::errors::FlezhaError;
use serde_json::value::RawValue;

/// One call's outcome with its request and answer still bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub request: Vec<u8>,
    pub result: Result<Vec<u8>, FlezhaError>,
    pub target: String,
    pub created: bool,
}

#[derive(serde::Deserialize)]
struct Wire<'a> {
    target: String,
    created: bool,
    request: String,
    #[serde(default)]
    value: Option<String>,
    #[serde(default, borrow)]
    error: Option<&'a RawValue>,
}

impl Envelope {
    /// Read one Response channel payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        Self::read(bytes).map(|(envelope, _)| envelope)
    }

    /// [`Envelope::decode`], with the error object's JSON text as `bytes`
    /// carry it, when the call ended in an error.
    pub fn read(bytes: &[u8]) -> Result<(Self, Option<&str>), String> {
        let wire: Wire =
            serde_json::from_slice(bytes).map_err(|e| format!("a Response envelope: {e}"))?;
        let result = match (wire.value, wire.error) {
            (_, Some(error)) => Err(FlezhaError::from_object(error.get())
                .ok_or("a Response error that is not an error document")?),
            (Some(value), None) => Ok(base64_decode(&value)?),
            (None, None) => return Err("a Response with neither a value nor an error".into()),
        };
        let envelope = Self {
            request: base64_decode(&wire.request)?,
            result,
            target: wire.target,
            created: wire.created,
        };
        Ok((envelope, wire.error.map(RawValue::get)))
    }
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard padded base64, the alphabet the controller reads.
pub fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let acc = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, &b)| acc | u32::from(b) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(BASE64[(acc >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Standard padded base64, the alphabet the controller writes.
pub fn base64_decode(text: &str) -> Result<Vec<u8>, String> {
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
    fn base64_of_every_length_reads_back_and_round_trips() {
        for (text, bytes) in [
            ("", &b""[..]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("AP8QgA==", &[0, 255, 16, 128]),
        ] {
            assert_eq!(base64_decode(text).as_deref(), Ok(bytes), "{text}");
        }
        for bytes in [&b""[..], b"f", b"fo", b"foo", &[0, 255, 16, 128]] {
            assert_eq!(base64_decode(&base64_encode(bytes)).as_deref(), Ok(bytes));
        }
        assert_eq!(base64_encode(&[0, 255, 16, 128]), "AP8QgA==");
        for bad in ["Zg=", "Z===x", "Zm9*", "=Zm9"] {
            assert!(base64_decode(bad).is_err(), "{bad}");
        }
    }
}
