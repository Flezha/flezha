//! A call's outcome as its Response channel delivers it, read by the core's
//! one decoder, so a host keeps only its codecs and its `Response` type.

use flz_core::call::Envelope;
use flz_core::FlezhaError;

use crate::{hand_out, set_init_error, Error, FLZ_ERR, FLZ_OK};

/// One decoded `Response` envelope as [`flz_core_response_decode`] hands it out,
/// in postcard: the answer's bytes, or the JSON error document the call ended in.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ResponseParts<'a> {
    pub target: &'a str,
    pub created: bool,
    pub request: &'a [u8],
    #[serde(borrow)]
    pub result: Result<&'a [u8], &'a str>,
}

fn encode(envelope: &Envelope) -> Result<Vec<u8>, String> {
    let document = envelope.result.as_ref().err().map(FlezhaError::json);
    let result = envelope
        .result
        .as_deref()
        .map_err(|_| document.as_deref().unwrap_or_default());
    let (target, request) = (envelope.target.as_str(), envelope.request.as_slice());
    let parts = ResponseParts {
        target,
        created: envelope.created,
        request,
        result,
    };
    postcard::to_allocvec(&parts).map_err(|e| e.to_string())
}

/// Decode one `Response` channel payload into `*out`, freed with
/// [`crate::flz_core_buf_free`]. A payload that is no call envelope answers
/// [`FLZ_ERR`], `call.malformed_response` in [`crate::flz_core_init_error`].
///
/// # Safety
/// `bytes`/`len` describe a readable buffer or `bytes` is NULL; `out` and
/// `out_len` are valid writable pointers.
#[no_mangle]
pub unsafe extern "C" fn flz_core_response_decode(
    bytes: *const u8,
    len: usize,
    out: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if out.is_null() || out_len.is_null() {
        return FLZ_ERR;
    }
    let payload = if bytes.is_null() {
        &[][..]
    } else {
        std::slice::from_raw_parts(bytes, len)
    };
    match Envelope::decode(payload).and_then(|envelope| encode(&envelope)) {
        Ok(encoded) => {
            hand_out(encoded, out, out_len);
            FLZ_OK
        }
        Err(reason) => {
            set_init_error(Error::MalformedResponse(reason));
            FLZ_ERR
        }
    }
}

#[cfg(test)]
mod tests {
    use flz_core::call::Envelope;

    use crate::veneer::{response_decode, Refusal};

    /// Every case of the shared fixture crosses the ABI as the core reads it.
    #[test]
    fn every_envelope_case_crosses_the_abi() {
        let fixture: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string("../test/fixtures/call_response_envelope.json")
                .expect("the shared fixture"),
        )
        .expect("JSON");
        for case in fixture["cases"].as_array().expect("cases") {
            let payload = case["payload"].as_str().expect("payload").as_bytes();
            let envelope = Envelope::decode(payload).expect("a case decodes");
            let crossed = response_decode(payload, |parts| {
                let result = parts.result.map(<[u8]>::to_vec).map_err(str::to_string);
                (
                    parts.target.to_string(),
                    parts.created,
                    parts.request.to_vec(),
                    result,
                )
            })
            .expect("a case crosses");
            let result = envelope.result.map_err(|error| error.json());
            let expected = (envelope.target, envelope.created, envelope.request, result);
            assert_eq!(crossed, expected, "{}", case["name"]);
        }
    }

    #[test]
    fn a_payload_that_is_no_envelope_is_malformed() {
        for payload in [
            &b"not json"[..],
            br#"{"target":"t","created":false,"request":""}"#,
        ] {
            let Err(Refusal::Core(document)) = response_decode(payload, |_| ()) else {
                panic!("{payload:?} is refused by the core");
            };
            let error = flz_core::FlezhaError::from_document(&document).expect("a document");
            assert_eq!(error.code, "call.malformed_response");
        }
    }
}
