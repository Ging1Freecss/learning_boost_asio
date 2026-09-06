#pragma once
#include <array>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <chrono>
#include <cstddef>
#include <iostream>

namespace timeout_watchdog {
using namespace boost::asio::experimental::awaitable_operators;
using boost::asio::ip::tcp;
using time_point = std::chrono::steady_clock::time_point;

auto echo(tcp::socket &socket_, time_point &deadline)
    -> boost::asio::awaitable<void> {
  std::array<char, 4196> data;

  while (true) {
    // create deadline of 10sec for the entire single loop of coroutine
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);

    std::size_t n = co_await socket_.async_read_some(
        boost::asio::buffer(data), boost::asio::use_awaitable);

    co_await boost::asio::async_write(socket_, boost::asio::buffer(data, n),
                                      boost::asio::use_awaitable);
  }
}

/*
    getting deadline by reference is important as
    it keep getting updated
*/
auto watchdog(time_point &deadline) -> boost::asio::awaitable<void> {
  boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

  auto now = std::chrono::steady_clock::now();

  while (deadline > now) {
    timer.expires_at(deadline);
    co_await timer.async_wait(boost::asio::use_awaitable);

    /*
        if the cliet sent  data and we send it back when timer is sleeping
       ,deadline = deadling + 10, in echo

       else deadline > now becomes false coroutine throw time_out
    */

    now = std::chrono::steady_clock::now();
  }
  throw boost::system::system_error(std::make_error_code(std::errc::timed_out));
}

auto handle_connection(tcp::socket sock) -> boost::asio::awaitable<void> {
  time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  try {
    co_await (echo(sock, deadline) && watchdog(deadline));
  } catch (const boost::system::system_error &e) {
    if (e.code() == std::errc::timed_out) {
      std::cout << "Client timed out (inactive for 10s)\n";
    } else if (e.code() == boost::asio::error::eof) {
      std::cout << "Client disconnected cleanly\n";
    } else {
      std::cerr << "Connection error: " << e.what() << "\n";
    }
  }
}

auto listen(tcp::acceptor &acceptor) -> boost::asio::awaitable<void> {
  while (true) {
    boost::asio::co_spawn(acceptor.get_executor(),
                          handle_connection(co_await acceptor.async_accept(
                              boost::asio::use_awaitable)),
                          boost::asio::detached);
  }
}

auto start_timeout_watchdog(boost::asio::io_context &ctx, int argc,
                            char *argv[]) -> int {
  try {
    if (argc != 2) {
      std::cerr << "Usage: <port>\n";
      return 1;
    }
    unsigned short port = static_cast<unsigned short>(std::atoi(argv[1]));
    tcp::acceptor acceptor(ctx, {tcp::v4(), port});
    co_spawn(ctx, listen(acceptor), boost::asio::detached);
    ctx.run();
  } catch (const std::exception &e) {
    std::cerr << "Exception in main: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
} // namespace timeout_watchdog
