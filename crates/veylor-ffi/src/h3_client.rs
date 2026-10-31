use std::boxed::Box;
use std::net::{IpAddr, SocketAddr};
use std::ptr;
use std::slice;
use std::str::FromStr;
use std::time::Duration;

use veylor_h3::bytes::Bytes;
use veylor_h3::http::{Method, Request, Uri};
use veylor_h3::{Client, ClientConfig, ClientError, ClientTls, Limits};

use crate::abi::{VylBytes, VylResult, catch_ffi, slice_length_valid};

pub const VYL_H3_CLIENT_ABI_VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct VylH3ClientConfig {
    pub abi_version: u32,
    pub reserved0: u32,
    pub struct_size: usize,
    pub address: VylBytes,
    pub server_name: VylBytes,
    pub root_cert: VylBytes,
    pub port: u16,
    pub reserved1: u16,
    pub reserved2: u32,
    pub idle_timeout_ns: u64,
    pub max_response_body: u64,
}

pub struct VylH3Client {
    client: Client,
}

pub struct VylH3Response {
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_client_new(
    config: *const VylH3ClientConfig,
    out_client: *mut *mut VylH3Client,
) -> VylResult {
    catch_ffi(|| unsafe { create(config, out_client) })
}

unsafe fn create(config: *const VylH3ClientConfig, out_client: *mut *mut VylH3Client) -> VylResult {
    if config.is_null() || out_client.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_client.write(ptr::null_mut());
    }
    let (abi_version, struct_size) = unsafe {
        (
            ptr::addr_of!((*config).abi_version).read(),
            ptr::addr_of!((*config).struct_size).read(),
        )
    };
    if abi_version != VYL_H3_CLIENT_ABI_VERSION
        || struct_size != std::mem::size_of::<VylH3ClientConfig>()
    {
        return VylResult::InvalidArgument;
    }
    let config = unsafe { *config };
    let address = match unsafe { read_bytes(config.address) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    let server_name = match unsafe { read_bytes(config.server_name) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    let root_cert = match unsafe { read_bytes(config.root_cert) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    if address.is_empty() || server_name.is_empty() || root_cert.is_empty() || config.port == 0 {
        return VylResult::InvalidArgument;
    }
    let Ok(address) = std::str::from_utf8(address) else {
        return VylResult::InvalidArgument;
    };
    let Ok(ip) = IpAddr::from_str(address) else {
        return VylResult::InvalidArgument;
    };
    let Ok(server_name) = std::str::from_utf8(server_name) else {
        return VylResult::InvalidArgument;
    };
    let tls = match ClientTls::from_der(root_cert) {
        Ok(tls) => tls,
        Err(_) => return VylResult::InvalidArgument,
    };
    let mut limits = Limits::default();
    if config.idle_timeout_ns != 0 {
        limits.idle_timeout = Duration::from_nanos(config.idle_timeout_ns);
    }
    if config.max_response_body != 0 {
        limits.max_response_body = usize::try_from(config.max_response_body)
            .unwrap_or(Limits::default().max_response_body);
    }
    let client = match Client::connect(ClientConfig {
        server: SocketAddr::new(ip, config.port),
        server_name: server_name.to_owned(),
        tls,
        limits,
    }) {
        Ok(client) => client,
        Err(error) => return client_error(error),
    };
    unsafe {
        out_client.write(Box::into_raw(Box::new(VylH3Client { client })));
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_client_request(
    client: *mut VylH3Client,
    method: VylBytes,
    url: VylBytes,
    body: VylBytes,
    out_response: *mut *mut VylH3Response,
) -> VylResult {
    catch_ffi(|| unsafe { request(client, method, url, body, out_response) })
}

unsafe fn request(
    client: *mut VylH3Client,
    method: VylBytes,
    url: VylBytes,
    body: VylBytes,
    out_response: *mut *mut VylH3Response,
) -> VylResult {
    if client.is_null() || out_response.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_response.write(ptr::null_mut());
    }
    let method = match unsafe { read_bytes(method) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    let url = match unsafe { read_bytes(url) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    let body = match unsafe { read_bytes(body) } {
        Ok(bytes) => bytes,
        Err(error) => return error,
    };
    let Ok(method) = Method::from_bytes(method) else {
        return VylResult::InvalidArgument;
    };
    let Ok(url) = std::str::from_utf8(url) else {
        return VylResult::InvalidArgument;
    };
    let Ok(uri) = Uri::try_from(url) else {
        return VylResult::InvalidArgument;
    };
    let mut request = Request::new(());
    *request.method_mut() = method;
    *request.uri_mut() = uri;
    let client = unsafe { &*client };
    let response = match client.client.request(request, Bytes::copy_from_slice(body)) {
        Ok(response) => response,
        Err(error) => return client_error(error),
    };
    let headers = response
        .headers
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect();
    let response = Box::new(VylH3Response {
        status: response.status.as_u16(),
        headers,
        body: response.body.to_vec(),
    });
    unsafe {
        out_response.write(Box::into_raw(response));
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_client_destroy(client: *mut VylH3Client) -> VylResult {
    if client.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        drop(Box::from_raw(client));
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_response_status(response: *const VylH3Response) -> u16 {
    if response.is_null() {
        return 0;
    }
    unsafe { (*response).status }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_response_header_count(response: *const VylH3Response) -> usize {
    if response.is_null() {
        return 0;
    }
    unsafe { (*response).headers.len() }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_response_header(
    response: *const VylH3Response,
    index: usize,
    out_name: *mut VylBytes,
    out_value: *mut VylBytes,
) -> VylResult {
    if response.is_null() || out_name.is_null() || out_value.is_null() {
        return VylResult::InvalidArgument;
    }
    let response = unsafe { &*response };
    let Some((name, value)) = response.headers.get(index) else {
        return VylResult::InvalidArgument;
    };
    unsafe {
        out_name.write(VylBytes {
            data: name.as_ptr(),
            length: name.len(),
        });
        out_value.write(VylBytes {
            data: value.as_ptr(),
            length: value.len(),
        });
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_response_body(
    response: *const VylH3Response,
    out_body: *mut VylBytes,
) -> VylResult {
    if response.is_null() || out_body.is_null() {
        return VylResult::InvalidArgument;
    }
    let response = unsafe { &*response };
    unsafe {
        out_body.write(VylBytes {
            data: response.body.as_ptr(),
            length: response.body.len(),
        });
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h3_response_destroy(response: *mut VylH3Response) -> VylResult {
    if response.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        drop(Box::from_raw(response));
    }
    VylResult::Ok
}

unsafe fn read_bytes(bytes: VylBytes) -> Result<&'static [u8], VylResult> {
    if bytes.data.is_null() {
        return if bytes.length == 0 {
            Ok(&[])
        } else {
            Err(VylResult::InvalidArgument)
        };
    }
    if !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}

fn client_error(error: ClientError) -> VylResult {
    match error {
        ClientError::Io(_) => VylResult::InternalError,
        ClientError::Config(_) => VylResult::InvalidArgument,
        ClientError::Connect(_) | ClientError::Closed => VylResult::ProtocolError,
        ClientError::Protocol(_) => VylResult::ProtocolError,
        ClientError::TooLarge => VylResult::BodyTooLong,
    }
}
