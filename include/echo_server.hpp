
#include <array>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <exception>

namespace echo_server {
using boost::asio::ip::tcp;
auto echo(tcp::socket socket_) -> boost::asio::awaitable<void> {
  try {
    std::array<char, 1024> data;

    while (true) {
      std::size_t n = co_await socket_.async_read_some(
          boost::asio::buffer(data), boost::asio::use_awaitable);

      co_await boost::asio::async_write(socket_, boost::asio::buffer(data, n),
                                        boost::asio::use_awaitable);
    }
  } catch (std::exception &e) {
    std::printf("echo Exception: %s\n", e.what());
  }
}

auto listener(unsigned short port) -> boost::asio::awaitable<void> {

  auto ex = co_await boost::asio::this_coro::executor;

  tcp::acceptor acceptor_(ex, {tcp::v4(), port});

  while (true) {
    tcp::socket socket_ =
        co_await acceptor_.async_accept(boost::asio::use_awaitable);

    boost::asio::co_spawn(ex, echo(std::move(socket_)), boost::asio::detached);
  }
}
auto start_echo_server(boost::asio::io_context &io_context, unsigned short port)
    -> int {

  try {
    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);

    signals.async_wait([&](auto...) { io_context.stop(); });

    boost::asio::co_spawn(io_context, listener(port), boost::asio::detached);
    io_context.run();
  } catch (std::exception &e) {
    std::printf("Exception: %s\n", e.what());
    return 1;
  }

  return 0;
}
} // namespace echo_server
