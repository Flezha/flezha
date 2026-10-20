//! A call's outcome as its Response channel delivers it, read by the core's
//! one decoder, so a host keeps only its codecs and its `Response` type.

use flz_core::call::Envelope;
use flz_core::Code;

use crate::{set_init_error, Error, FLZ_ERR, FLZ_OK};

/// Decode one `Response` channel payload into `out`, which holds `len` bytes:
/// the target, the request and the result back to back, the result being the
/// answer's bytes or the JSON error document the call ended in, as the payload
/// carries it. `parts` receives their three lengths, then `created` and
/// `answered`, each 0 or 1. A payload that is no call envelope answers
/// [`FLZ_ERR`], `call.malformed_response` in [`crate::flz_core_init_error`].
///
/// # Safety
/// `bytes` is NULL or holds `len` bytes; `out` holds `len` writable bytes and
/// `parts` five `usize`s, neither overlapping `bytes`.
#[no_mangle]
pub unsafe extern "C" fn flz_core_response_decode(
    bytes: *const u8,
    len: usize,
    out: *mut u8,
    parts: *mut usize,
) -> i32 {
    if out.is_null() || parts.is_null() {
        return FLZ_ERR;
    }
    let payload = if bytes.is_null() {
        &[][..]
    } else {
        std::slice::from_raw_parts(bytes, len)
    };
    let out = std::slice::from_raw_parts_mut(out, len);
    match Envelope::read(payload).and_then(|(envelope, error)| {
        let (target, request) = (envelope.target.as_bytes(), &envelope.request[..]);
        let result: &[&[u8]] = match (error, &envelope.result) {
            (Some(object), _) => &[b"{\"error\":", object.as_bytes(), b"}"],
            (None, value) => &[value.as_deref().unwrap_or_default()],
        };
        let mut at = 0;
        for part in [target, request].iter().chain(result) {
            let slot = out.get_mut(at..at + part.len());
            slot.ok_or("a decoded Response that outgrew its payload")?
                .copy_from_slice(part);
            at += part.len();
        }
        let (created, answered) = (envelope.created.into(), error.is_none().into());
        let result_len = at - target.len() - request.len();
        *parts.cast::<[usize; 5]>() = [target.len(), request.len(), result_len, created, answered];
        Ok(())
    }) {
        Ok(()) => FLZ_OK,
        Err(reason) => {
            let reason = [("reason", reason.into())];
            set_init_error(Error::Refused(Code::MalformedResponse.with(reason)));
            FLZ_ERR
        }
    }
}

#[cfg(test)]
mod tests {
    use flz_core::call::Envelope;

    use crate::veneer::{response_decode, response_into, Refusal};

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
            let crossed = (
                crossed.0,
                crossed.1,
                crossed.2,
                crossed.3.map_err(|document| {
                    flz_core::FlezhaError::from_document(&document).expect("an error document")
                }),
            );
            let expected = (
                envelope.target,
                envelope.created,
                envelope.request,
                envelope.result,
            );
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

    #[test]
    fn a_buffer_shorter_than_the_payload_is_refused() {
        let payload = br#"{"target":"t","created":false,"request":"","value":""}"#;
        let mut out = vec![0; payload.len() - 1];
        let refused = response_into(payload, &mut out, &mut [0; 5]);
        assert!(matches!(refused, Err(Refusal::Shape(_))), "{refused:?}");
    }
}
