//! Exposes `LokiTableProvider` across a C ABI boundary via `datafusion-ffi`'s
//! `FFI_TableProvider`, for consumption by the `datafusion-go` Go binding.
//!
//! This is a prototype: the goal is to let a Go process query Loki through
//! DataFusion **in-process**, with no separate HTTP bridge server in between
//! (see `bridge/` for the existing, working HTTP-based approach this is
//! meant to eventually replace). `datafusion-go` requires an exact
//! producer/consumer DataFusion version match (`=53.1.0`, matching
//! `DataFusionVersion` in the Go package) before it will even dereference the
//! provider pointer — see `RegisterFFITableProvider`'s doc comment in that
//! package for the version-handshake contract this crate must satisfy.
//!
//! ## Ownership contract (from datafusion-ffi's own docs)
//!
//! The `FFI_TableProvider` returned by [`create_loki_provider`] must be
//! memory owned by *this* library, not the Go heap, because the foreign
//! (Go/DataFusion) side retains function pointers cloned out of it and calls
//! back into them on every query. This library must therefore stay loaded
//! for as long as the Go side keeps a registered table backed by it.

use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContextProvider;
use datafusion::prelude::SessionContext;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_loki::{LokiConfig, LokiTableProvider};
use once_cell::sync::OnceCell;

/// A single, process-wide Tokio runtime driving every Loki HTTP call made
/// through providers created by this library. `datafusion-ffi` needs a
/// `tokio::runtime::Handle` so foreign (non-async) callers can still execute
/// our async `ExecutionPlan` — see `FFI_TableProvider::new`'s `runtime`
/// parameter.
static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build bifrost-ffi-export's Tokio runtime")
    })
}

/// Owns the `SessionContext` a created `FFI_TableProvider` is registered
/// against. `datafusion-ffi` needs a `TaskContextProvider` (satisfied by
/// `SessionContext` itself) to construct the `TaskContext` foreign scans
/// execute under; wrapping it once and reusing it avoids creating a new
/// (empty) session per query.
static SESSION: OnceCell<Arc<SessionContext>> = OnceCell::new();

fn session() -> Arc<SessionContext> {
    SESSION.get_or_init(|| Arc::new(SessionContext::new())).clone()
}

/// Builds a `LokiTableProvider` from the given base URL and LogQL stream
/// selector, wraps it as an `FFI_TableProvider`, and returns a raw pointer to
/// it. The caller (Go, via `datafusion-go`'s `RegisterFFITableProvider`)
/// takes a *cloned* reference when registering — this library retains
/// ownership of the returned pointer and must free it via
/// [`bifrost_ffi_free_provider`] once done.
///
/// # Safety
/// `base_url` and `stream_selector` must be valid, NUL-terminated C strings.
/// The returned pointer must eventually be passed to
/// [`bifrost_ffi_free_provider`] exactly once, and must not be dereferenced
/// after that call.
#[no_mangle]
pub unsafe extern "C" fn bifrost_ffi_create_provider(
    base_url: *const c_char,
    stream_selector: *const c_char,
    labels_csv: *const c_char,
) -> *mut FFI_TableProvider {
    let base_url = match unsafe { CStr::from_ptr(base_url) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let stream_selector = match unsafe { CStr::from_ptr(stream_selector) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let labels_csv = match unsafe { CStr::from_ptr(labels_csv) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let labels: Vec<String> = labels_csv
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();

    let config = LokiConfig::new(base_url, stream_selector);
    let provider: Arc<dyn TableProvider + Send> = Arc::new(LokiTableProvider::new(config, labels));

    let ctx = session();
    let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;

    let ffi_provider = FFI_TableProvider::new(
        provider,
        true, // can_support_pushdown_filters — LokiTableProvider implements supports_filters_pushdown
        Some(runtime().handle().clone()),
        &task_ctx_provider,
        None,
    );

    Box::into_raw(Box::new(ffi_provider))
}

/// Frees a provider created by [`bifrost_ffi_create_provider`].
///
/// # Safety
/// `provider` must be a pointer previously returned by
/// [`bifrost_ffi_create_provider`], not yet freed, and not used again after
/// this call.
#[no_mangle]
pub unsafe extern "C" fn bifrost_ffi_free_provider(provider: *mut FFI_TableProvider) {
    if !provider.is_null() {
        drop(unsafe { Box::from_raw(provider) });
    }
}

/// Returns the DataFusion version string this library was built against, as
/// a NUL-terminated C string owned by this library (do not free). Go should
/// assert this equals `datafusion.DataFusionVersion` before trusting a
/// provider pointer from this library — `RegisterFFITableProvider` already
/// enforces this, but exposing it directly makes the constraint checkable
/// without going through a failed registration first.
#[no_mangle]
pub extern "C" fn bifrost_ffi_datafusion_version() -> *const c_char {
    // Built from the exact version this crate pins datafusion/datafusion-ffi to.
    static VERSION: &CStr = c"53.1.0";
    VERSION.as_ptr()
}

/// Opaque handle type alias for documentation purposes at the Go call site;
/// not used directly in Rust.
#[allow(dead_code)]
type OpaqueProvider = c_void;
