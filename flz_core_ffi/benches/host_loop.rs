//! The erased-path twin of `sdk/rust/benches/dispatch.rs`'s
//! `regression_100x1x1x10k`: the same shape (100 instances × 1 procedure × 1
//! entity × 10K inputs, no change), fed through the same
//! `OperativeDispatchHandle`, but the class is a `HostClassDecl` whose
//! activations leave through `flz_core_poll` and come back through
//! `flz_core_complete` from a fixed set of fetcher threads — `to_schema` →
//! encode → decode → `apply_result` → `resolve_item`, with no veneer in the
//! loop. Drives the ABI exactly as `tests/abi_scenario.rs` does.

#[path = "alloc_counter.rs"]
mod alloc_counter;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flz_core::bench_support::{fan_in, measurement_secs, ok_effect, stream_pair, Settle};
use flz_core::operative::instance::StreamInstance;
use flz_core::operative::registry::{get_class, OperativeRegistry};
use flz_core::runner::OperativeDispatchHandle;
use flz_core::schema;
use flz_core_ffi::*;

/// The host's fetcher pool, sized like the typed bench's poll pool on this box.
const FETCHERS: usize = 8;

/// Items settled through `flz_core_complete`.
static DONE: Settle = Settle::new();

fn last_error(ctx: *mut FlzContext) -> String {
    let err = unsafe { flz_core_last_error(ctx) };
    if err.is_null() {
        return "<no error recorded>".to_string();
    }
    unsafe { std::ffi::CStr::from_ptr(err) }
        .to_string_lossy()
        .into_owned()
}

/// One host fetcher: poll, decode the activation as a veneer would, answer
/// every item `Ok`, complete.
fn fetch_loop(ctx: usize, stop: Arc<AtomicBool>) {
    let ctx = ctx as *mut FlzContext;
    while !stop.load(Ordering::Relaxed) {
        let mut buf: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        match unsafe { flz_core_poll(ctx, 100, &mut buf, &mut len) } {
            FLZ_POLL_ACTIVATION => {
                let act =
                    schema::decode_activation(unsafe { std::slice::from_raw_parts(buf, len) })
                        .expect("activation decodes");
                unsafe { flz_core_buf_free(buf, len) };
                let items = match &act.kind {
                    schema::ActivationKind::Work(items) => items.len(),
                    other => panic!("unexpected activation kind: {other:?}"),
                };
                let result = schema::ActivationResult {
                    token: act.token,
                    outcome: schema::ActivationOutcome::Work(
                        (0..items).map(|_| ok_effect()).collect(),
                    ),
                };
                let bytes = schema::encode_result(&result);
                let status = unsafe { flz_core_complete(ctx, bytes.as_ptr(), bytes.len()) };
                assert_eq!(status, FLZ_OK, "complete failed: {}", last_error(ctx));
                DONE.add(items);
            }
            FLZ_POLL_TIMEOUT => {}
            FLZ_POLL_SHUTTING_DOWN => break,
            other => panic!("poll failed ({other}): {}", last_error(ctx)),
        }
    }
}

fn host_loop_bench(c: &mut Criterion) {
    let n_instances = 100usize;
    let msgs_per_instance = 10_000usize;
    let total = n_instances * msgs_per_instance;
    let measurement_secs = measurement_secs();

    let ctx = flz_core_context_new(flz_core::SCHEMA_VERSION);
    assert!(!ctx.is_null(), "context creation");
    // One entry channel, one wall-clock single-entity procedure, no state:
    // the erased twin of `RegressionOperative`. The entry channel keeps the
    // `drop_nothing` default the typed bench opts into explicitly.
    let decl = br#"{
        "factory_id": "host_loop_bench",
        "sdk_lang": 1,
        "channels": [{"name": "input", "kind": "entry", "consumers": [0]}],
        "procedures": [{"name": "host_pass"}]
    }"#;
    assert_eq!(
        unsafe { flz_core_declare_class(ctx, decl.as_ptr(), decl.len()) },
        FLZ_OK,
        "declare failed: {}",
        last_error(ctx)
    );
    let topology = get_class("host_loop_bench").expect("the declared class");
    let registry = Arc::new(OperativeRegistry::new());
    let instance_ids: Vec<String> = (0..n_instances).map(|i| format!("host-loop-{i}")).collect();
    for id in &instance_ids {
        registry.register_stream(StreamInstance::new(
            Arc::clone(&topology),
            Arc::from(id.as_str()),
        ));
    }
    let handle = OperativeDispatchHandle { registry };
    let stream = stream_pair().0;
    let payload: Vec<u8> = b"x".repeat(64);

    let stop = Arc::new(AtomicBool::new(false));
    let ctx_addr = ctx as usize;
    let fetchers: Vec<_> = (0..FETCHERS)
        .map(|_| {
            let stop = Arc::clone(&stop);
            thread::spawn(move || fetch_loop(ctx_addr, stop))
        })
        .collect();

    let mut group = c.benchmark_group("host_dispatch");
    group.measurement_time(Duration::from_secs(measurement_secs));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(total as u64));
    group.bench_function("regression_host_100x1x1x10k", |b| {
        b.iter_custom(|iters| {
            let allocs_before = alloc_counter::allocations();
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                DONE.reset();
                let start = Instant::now();
                fan_in(&handle, &instance_ids, msgs_per_instance, &payload, &stream);
                let drained = DONE.wait_for(total, Duration::from_secs(60));
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "host loop drain timed out: {} of {}",
                    DONE.current(),
                    total
                );
            }
            alloc_counter::report(
                "regression_host_100x1x1x10k",
                allocs_before,
                (iters as usize * total) as u64,
            );
            elapsed
        });
    });
    group.finish();

    stop.store(true, Ordering::Relaxed);
    for fetcher in fetchers {
        fetcher.join().expect("fetcher joins");
    }
    unsafe { flz_core_context_free(ctx) };
}

criterion_group!(benches, host_loop_bench);
criterion_main!(benches);
