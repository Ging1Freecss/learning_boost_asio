
// Provides `boost::asio::deferred`, a completion token used for lazy/deferred
// initiation of asynchronous operations.
#include <boost/asio/async_result.hpp>
#include <boost/asio/deferred.hpp>
// Provides `boost::asio::io_context`, the core I/O execution context / event
// loop that dispatches completion handlers.
#include <boost/asio/io_context.hpp>
// Provides TCP networking primitives: `tcp::socket`, `tcp::acceptor`, and
// `tcp::endpoint`.
#include <boost/asio/ip/tcp.hpp>
// Provides `boost::asio::use_future`, a completion token that transforms the
// asynchronous operation into a `std::future`.
#include <boost/asio/use_future.hpp>
// Provides `boost::asio::async_write`, a high-level composite operation that
// guarantees the entire buffer is written unless an error occurs (unlike
// socket.async_write_some).
#include <boost/asio/write.hpp>
// Standard C/C++ library headers for string length, console output, and move
// semantics.
#include <boost/system/detail/error_code.hpp>
#include <cstddef>
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <type_traits>
#include <utility>

namespace compose {
using boost::asio::ip::tcp;

template <typename CompletionToken>
  requires boost::asio::completion_token_for<
      CompletionToken, void(boost::system::error_code, std::size_t)>
auto async_write_message(tcp::socket &socket, const char *message,
                         CompletionToken &&token) {
  return boost::asio::async_write(
      socket, boost::asio::buffer(message, std::strlen(message)),
      std::forward<CompletionToken>(token));
}

void test_callback() {
  boost::asio::io_context io_context;
  tcp::acceptor acceptor(io_context, {tcp::v4(), 55555});
  tcp::socket socket = acceptor.accept();

  async_write_message(
      socket, "Testing callback\r\n",
      [](const boost::system::error_code &error, std::size_t n) {
        if (!error) {
          std::cout << n << " bytes transferred\n";
        } else {
          std::cout << "Error: " << error.message() << "\n";
        }
      });

  io_context.run();
}

void test_deferred() {
  boost::asio::io_context io_context;

  tcp::acceptor acceptor(io_context, {tcp::v4(), 55555});
  tcp::socket socket = acceptor.accept();

  boost::asio::async_operation auto op = async_write_message(
      socket, "Testing deferred\r\n", boost::asio::deferred);

  std::move(op)([](const boost::system::error_code &error, std::size_t n) {
    if (!error) {
      std::cout << n << " bytes transferred\n";
    } else {
      std::cout << "Error: " << error.message() << "\n";
    }
  });

  io_context.run();
}

void test_future() {

  boost::asio::io_context io_context;
  tcp::acceptor acceptor(io_context, {tcp::v4(), 55555});
  tcp::socket socket = acceptor.accept();
  std::future<std::size_t> f = async_write_message(socket, "Testing future\r\n",
                                                   boost::asio::use_future);
  io_context.run();
  try {
    // Retrieve the result. Because `io_context.run()` already finished all
    // work, the value is already available and `f.get()` returns immediately
    // without blocking.
    std::size_t n = f.get();
    std::cout << n << " bytes transferred\n";
  } catch (const std::exception &e) {
    // If the socket was closed or an OS error occurred during the write,
    // it arrives here as a `boost::system::system_error`.
    std::cout << "Error: " << e.what() << "\n";
  }
}

void start_compose() {
  test_callback();
  test_deferred();
  test_future();
}
} // namespace compose