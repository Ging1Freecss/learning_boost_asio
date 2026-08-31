#include "timer_sequence.hpp"
#include <print>

using asio::use_awaitable;
using namespace std::chrono;

auto now() -> std::int64_t {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
      .count();
}

auto timer_sequence() -> asio::awaitable<void> {
  auto executor = co_await asio::this_coro::executor;
  auto start = now();

  std::println("[{:>6}ms] coroutine started", now() - start);

  // First timer — 1 second
  asio::steady_timer t1(executor, 1s);
  co_await t1.async_wait(use_awaitable);
  std::println("[{:>6}ms] first timer fired (1s)", now() - start);

  // Second timer — 2 seconds
  asio::steady_timer t2(executor, 2s);
  co_await t2.async_wait(use_awaitable);
  std::println("[{:>6}ms] second timer fired (2s)", now() - start);

  std::println("[{:>6}ms] coroutine done!", now() - start);
}

void run_timer_sequence(executor_type ex) {
  using asio::co_spawn;
  using asio::detached;
  // Spawn the coroutine — 'detached' means fire-and-forget
  co_spawn(ex, timer_sequence(), detached);

 
}
