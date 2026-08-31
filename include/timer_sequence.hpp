#pragma once

#include <boost/asio.hpp>
#include <chrono>
#include <cstdint>

namespace asio = boost::asio;

/// Returns current time in milliseconds since epoch
auto now() -> std::int64_t;

/// A coroutine that awaits two timers sequentially
auto timer_sequence() -> asio::awaitable<void>;

using executor_type = boost::asio::any_io_executor;
void run_timer_sequence(executor_type ex);