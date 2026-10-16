package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:sys/linux"
import http1 "veylor:http1"
import router "veylor:router"
import connections "veylor:runtime/connections"
import runtime "veylor:runtime/linux"
import shutdown "veylor:runtime/shutdown"
import workers "veylor:runtime/workers"
import "veylor:transport"
import epoll "veylor:transport/linux"
import socket "veylor:transport/linux/socket"

main :: proc() {
	duration_seconds, port, valid := server_options()
	if !valid {
		fmt.eprintln("usage: veylor-static [duration-seconds:1..3600] [port:1..65535]")
		os.exit(2)
	}
	if !run_static_server(duration_seconds, port) {
		os.exit(1)
	}
}

server_options :: proc() -> (duration_seconds: int, port: u16, valid: bool) {
	duration_seconds = 60
	port = 8080
	args := os.args
	if len(args) > 1 {
		value, parsed := strconv.parse_int(args[1])
		if !parsed || value < 1 || value > 3600 {
			return 0, 0, false
		}
		duration_seconds = value
	}
	if len(args) > 2 {
		value, parsed := strconv.parse_int(args[2])
		if !parsed || value < 1 || value > 65_535 {
			return 0, 0, false
		}
		port = u16(value)
	}
	if len(args) > 3 {
		return 0, 0, false
	}
	return duration_seconds, port, true
}

run_static_server :: proc(duration_seconds: int, port: u16) -> bool {
	loopback := [4]u8{127, 0, 0, 1}
	listener, listen_result, listen_error := socket.listen_ipv4(
		loopback,
		port,
		128,
	)
	if listen_result != .Ready {
		fmt.eprintln("listen failed:", listen_result, listen_error)
		return false
	}

	poller, poll_result, poll_error := epoll.create()
	if poll_result != .Ready {
		fmt.eprintln("epoll create failed:", poll_result, poll_error)
		socket.close_listener(&listener)
		return false
	}

	route_storage: [1]router.Static_Route
	route_index_storage: [2]router.Route_Index_Slot
	routes := router.init_route_table(route_storage[:], route_index_storage[:])
	method := "GET"
	path := "/health"
	reason := "OK"
	response_body := "ok\n"
	static_response := router.Static_Response {
		status = 200,
		reason = transmute([]u8)reason,
		body   = transmute([]u8)response_body,
	}
	register_result := router.register_static_route(
		&routes,
		transmute([]u8)method,
		transmute([]u8)path,
		static_response,
	)
	if register_result != .Added || router.freeze_route_table(&routes) != .Frozen {
		fmt.eprintln("route setup failed:", register_result)
		epoll.close(&poller)
		socket.close_listener(&listener)
		return false
	}

	connection_slots: [128]connections.Connection_Slot
	connection_free_stack: [128]u32
	table, table_result := connections.init_connection_table(
		1,
		1,
		connection_slots[:],
		connection_free_stack[:],
	)
	if table_result != .Ready {
		fmt.eprintln("connection table setup failed:", table_result)
		epoll.close(&poller)
		socket.close_listener(&listener)
		return false
	}

	workspace_storage: [8]runtime.Static_Connection_Workspace
	request_input_storage: [8][16 * 1024]u8
	header_storage: [8][100]http1.Header
	response_storage: [8][1024]u8
	for i := 0; i < len(workspace_storage); i += 1 {
		workspace_storage[i] = runtime.Static_Connection_Workspace {
			request_input = request_input_storage[i][:],
			headers       = header_storage[i][:],
			response      = response_storage[i][:],
		}
	}
	workspace_free_stack: [8]u32
	workspace_pool, pool_ready := runtime.init_static_workspace_pool(
		workspace_storage[:],
		workspace_free_stack[:],
	)
	if !pool_ready {
		fmt.eprintln("workspace pool setup failed")
		epoll.close(&poller)
		socket.close_listener(&listener)
		return false
	}

	shutdown_state: shutdown.State
	timer_storage: [130]transport.Timer_Expiration
	timers := transport.init_timer_queue(timer_storage[:])
	connection_states: [128]runtime.Static_Connection
	native_events: [128]linux.EPoll_Event
	readiness_events: [128]transport.Readiness_Event
	accepted_storage: [16]socket.Accepted_Connection
	accepted_connections: [16]runtime.Worker_Connection
	expired_timers: [130]transport.Timer_Expiration
	worker := runtime.Static_Worker {
		poller                = poller,
		listener              = listener,
		shutdown_state        = &shutdown_state,
		table                 = &table,
		routes                = &routes,
		connection_states     = connection_states[:],
		workspace_pool        = &workspace_pool,
		native_events         = native_events[:],
		readiness_events      = readiness_events[:],
		accepted_storage      = accepted_storage[:],
		accepted_connections  = accepted_connections[:],
		timers                = &timers,
		expired_timer_storage = expired_timers[:],
		event_budget          = 128,
		byte_budget           = 64 * 1024,
		accept_attempt_budget = 16,
		timeout_ms            = 100,
		body_limit            = 16 * 1024 * 1024,
		timeouts              = connections.Connection_Timeouts {
			request_head_ns    = 10_000_000_000,
			response_write_ns  = 10_000_000_000,
			keep_alive_idle_ns = 5_000_000_000,
		},
	}
	worker_threads: [1]workers.Worker_Thread
	server: runtime.Managed_Server
	server_result := runtime.init_managed_server(
		&server,
		&worker,
		worker_threads[:],
		5_000_000_000,
	)
	if server_result != .Initialized {
		fmt.eprintln("server initialization failed:", server_result)
		epoll.close(&worker.poller)
		socket.close_listener(&worker.listener)
		return false
	}

	start_result := runtime.start_managed_server(&server)
	if start_result != .Started {
		fmt.eprintln("server start failed:", start_result)
		finish_result := finish_server(&server, true)
		return finish_result == .Destroyed
	}
	fmt.println("Listening on http://127.0.0.1:", port, "/health", sep="")

	started, clock_error := linux.clock_gettime(.MONOTONIC)
	if clock_error != .NONE {
		fmt.eprintln("clock read failed:", clock_error)
		return finish_server(&server, true) == .Destroyed
	}
	started_ns := u64(started.time_sec) * 1_000_000_000 + u64(started.time_nsec)
	deadline_ns := started_ns + u64(duration_seconds) * 1_000_000_000
	for {
		now, now_error := linux.clock_gettime(.MONOTONIC)
		if now_error != .NONE {
			fmt.eprintln("clock read failed:", now_error)
			return finish_server(&server, true) == .Destroyed
		}
		now_ns := u64(now.time_sec) * 1_000_000_000 + u64(now.time_nsec)
		if now_ns >= deadline_ns {
			break
		}
		sleep := linux.Time_Spec {
			time_sec  = 0,
			time_nsec = 100_000_000,
		}
		remaining: linux.Time_Spec
		sleep_error := linux.nanosleep(&sleep, &remaining)
		if sleep_error != .NONE && sleep_error != .EINTR {
			fmt.eprintln("sleep failed:", sleep_error)
			return finish_server(&server, true) == .Destroyed
		}
	}

	finish_result := finish_server(&server, false)
	if finish_result != .Destroyed {
		fmt.eprintln("server shutdown failed:", finish_result)
		return false
	}
	fmt.println(
		"accepted connections:", worker.metrics.accepted_connections,
		"bytes read:", worker.metrics.bytes_read,
		"bytes written:", worker.metrics.bytes_written,
		"workspace peak:", worker.workspace_pool.high_water_mark,
	)
	return true
}

finish_server :: proc(server: ^runtime.Managed_Server, force: bool) -> runtime.Managed_Server_Result {
	if server.phase == .Running {
		if force {
			runtime.request_managed_server_force_stop(server)
		} else {
			runtime.request_managed_server_stop(server)
		}
	}
	if server.phase == .Stop_Requested {
		runtime.join_managed_server(server)
	}
	if server.phase == .Joined || server.phase == .Unstarted {
		return runtime.destroy_managed_server(server)
	}
	return .Worker_Error
}
