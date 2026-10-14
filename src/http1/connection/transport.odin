package connection

import "veylor:transport"

valid_transport_result :: proc(result: transport.IO_Result) -> bool {
	if result.transferred_bytes < 0 {
		return false
	}

	if result.kind == .Progress {
		return result.error_code == 0
	}

	if result.transferred_bytes != 0 {
		return false
	}

	if result.kind == .Error {
		return result.error_code != 0
	}
	return result.error_code == 0
}
