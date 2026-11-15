use super::super::*;
use super::ffi_server::*;
use crate::abi::VylResult;
use veylor_server::MAX_WORKERS;

#[test]
fn serves_registered_static_routes_and_reports_close_counts() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    unsafe { register(server, b"GET", b"/data", b"abc") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };

    let hello = get(port, "/hello");
    assert!(
        hello.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {hello:?}"
    );
    assert!(hello.ends_with("hello"), "unexpected body: {hello:?}");
    let data = get(port, "/data");
    assert!(
        data.starts_with("HTTP/1.1 200 OK"),
        "unexpected head: {data:?}"
    );
    assert!(data.ends_with("abc"), "unexpected body: {data:?}");
    let missing = get(port, "/missing");
    assert!(
        missing.starts_with("HTTP/1.1 404 Not Found"),
        "unexpected head: {missing:?}"
    );

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 3);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_duplicate_routes_before_start() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/hello"),
                200,
                bytes(b"OK"),
                bytes(b"again"),
            )
        },
        VylResult::InvalidArgument
    );
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_registration_after_start_and_joins_cleanly() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_managed_static_register_static_route(
                server,
                bytes(b"GET"),
                bytes(b"/late"),
                200,
                bytes(b"OK"),
                bytes(b"late"),
            )
        },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_force(server) },
        VylResult::Ok
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::Ok
    );
    assert_eq!(counts_total(counts), 0);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn rejects_invalid_configs_before_touching_anything() {
    let mut null_server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(std::ptr::null(), &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());
    let config = test_config(0);
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );

    let mut bad = test_config(0);
    bad.abi_version = VYL_MANAGED_STATIC_ABI_VERSION + 1;
    assert_eq!(
        unsafe { vyl_managed_static_new(&bad, &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());

    bad = test_config(0);
    bad.struct_size = 0;
    assert_eq!(
        unsafe { vyl_managed_static_new(&bad, &mut null_server) },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());

    #[repr(C)]
    struct ShortConfig {
        abi_version: u32,
        struct_size: usize,
    }

    let short = ShortConfig {
        abi_version: VYL_MANAGED_STATIC_ABI_VERSION,
        struct_size: std::mem::size_of::<ShortConfig>(),
    };
    assert_eq!(
        unsafe {
            vyl_managed_static_new(
                &short as *const ShortConfig as *const VylManagedStaticConfig,
                &mut null_server,
            )
        },
        VylResult::InvalidArgument
    );
    assert!(null_server.is_null());

    for mutator in [
        (|config: &mut VylManagedStaticConfig| config.request_head_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.body_progress_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.response_write_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.keep_alive_idle_timeout_ns = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.max_accepts_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.max_events_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.byte_budget_per_turn = 0)
            as fn(&mut VylManagedStaticConfig),
        (|config: &mut VylManagedStaticConfig| config.poll_wait_ms = 0)
            as fn(&mut VylManagedStaticConfig),
    ] {
        bad = test_config(0);
        mutator(&mut bad);
        assert_eq!(
            unsafe { vyl_managed_static_new(&bad, &mut null_server) },
            VylResult::InvalidArgument
        );
        assert!(null_server.is_null());
    }
}

#[test]
fn rejects_null_handles_and_bad_state_transitions() {
    assert_eq!(
        unsafe { vyl_managed_static_destroy(std::ptr::null_mut()) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_start(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    let mut counts = VylManagedStaticCloseCounts::default();
    assert_eq!(
        unsafe { vyl_managed_static_join(std::ptr::null_mut(), &mut counts) },
        VylResult::InvalidArgument
    );

    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { vyl_managed_static_stop_accepting(server) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_join(server, &mut counts) },
        VylResult::BadState
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe { vyl_managed_static_start(server) },
        VylResult::BadState
    );
    assert_eq!(
        unsafe { vyl_managed_static_destroy(server) },
        VylResult::BadState
    );
    let joined = unsafe { stop_join(server) };
    assert_eq!(counts_total(joined), 0);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

const ROTATE_CERT1: &[u8] = include_bytes!("../../../../veylor-tls/tests/testdata/server_cert.der");

const ROTATE_KEY1: &[u8] = include_bytes!("../../../../veylor-tls/tests/testdata/server_key.der");

const ROTATE_CERT2: &[u8] =
    include_bytes!("../../../../veylor-tls/tests/testdata/server_cert2.der");

const ROTATE_KEY2: &[u8] = include_bytes!("../../../../veylor-tls/tests/testdata/server_key2.der");

unsafe fn enable_tls(server: *mut VylManagedStaticServer, cert: &[u8], key: &[u8]) {
    assert_eq!(
        unsafe {
            vyl_managed_static_enable_tls(
                server,
                cert.as_ptr(),
                cert.len(),
                key.as_ptr(),
                key.len(),
                16,
            )
        },
        VylResult::Ok
    );
}

#[test]
fn rotate_tls_validates_state_and_applies_on_running_server() {
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                ROTATE_CERT2.as_ptr(),
                ROTATE_CERT2.len(),
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::BadState
    );
    unsafe { enable_tls(server, ROTATE_CERT1, ROTATE_KEY1) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                ROTATE_CERT2.as_ptr(),
                ROTATE_CERT2.len(),
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::Ok
    );
    assert_eq!(
        unsafe {
            vyl_managed_static_rotate_tls(
                server,
                b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----".as_ptr(),
                55,
                ROTATE_KEY2.as_ptr(),
                ROTATE_KEY2.len(),
            )
        },
        VylResult::InvalidArgument
    );
    let _ = unsafe { stop_join(server) };
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn config_binds_address_and_applies_limits() {
    let mut config = test_config(0);
    let address = b"0.0.0.0";
    config.address = bytes(address);
    config.max_connections = 1;
    config.worker_count = 2;

    let server = unsafe { create(&config) };
    unsafe { register(server, b"GET", b"/hello", b"hello") };
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let port = unsafe { local_port(server) };
    let response = get(port, "/hello");
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let counts = unsafe { stop_join(server) };
    assert_eq!(counts_total(counts), 1);
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}

#[test]
fn config_rejects_invalid_address_and_limits() {
    let mut config = test_config(0);
    let bad_address = b"not-an-ip";
    config.address = bytes(bad_address);
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );

    let mut config = test_config(0);
    config.max_connections = 1025;
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );

    let mut config = test_config(0);
    config.worker_count = MAX_WORKERS as u64 + 1;
    let mut server = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_managed_static_new(&config, &mut server) },
        VylResult::InvalidArgument
    );
}
