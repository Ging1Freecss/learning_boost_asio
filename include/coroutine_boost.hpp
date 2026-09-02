#pragma once
#include <boost/asio.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/write.hpp>
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

namespace example_1 {
using boost::asio::experimental::channel;

class line_based_echo_session
    : public std::enable_shared_from_this<line_based_echo_session> {

private:
  using tcp = boost::asio::ip::tcp;
  /*
        tcp socket is for client connection ,
        supports duplex connection (1 active async_read and 1 active
     async_write)
  */
  boost::asio::ip::tcp::socket socket_;

  /*
      channel act as a binary semaphore for locking
      void() represent that it does not send any data but send
      only notification on the same event loop
      buffer empty:(0/1) -> unlock
      buffer empty:(1/1) -> lock
  */
  boost::asio::experimental::channel<void()> write_lock_{socket_.get_executor(),
                                                         1};

public:
  explicit line_based_echo_session(boost::asio::ip::tcp::socket socket)
      : socket_{std::move(socket)} {
    socket_.set_option(tcp::no_delay(true));
  }

  /*
    co_spawn , it is a bridge function that launches coroutine that
    return type awaitable<T> to asio executor like io_context

    it allocate frame ->  schedule initail execution and ensure resumption
    after co_await is dispatched back onto that executor
  */

  void start() {
    boost::asio::co_spawn(
        socket_.get_executor(),
        [self = shared_from_this()] { return self->handle_messages(); },
        boost::asio::detached);

    boost::asio::co_spawn(
        socket_.get_executor(),
        [self = shared_from_this()] { return self->send_heartbeats(); },
        boost::asio::detached);
  }

private:
  void stop() {
    if (socket_.is_open()) {
      socket_.close();
    }

    write_lock_.cancel();
  }

  auto handle_messages() -> boost::asio::awaitable<void> {

    using namespace boost::asio::buffer_literals;
    try {
      constexpr std::size_t max_line_length{1024};
      std::string data;

      while (true) {
        /*
          step 1:
          is to read the data till '\n'
          no lock during reading as tcp is full duplex it allows hearbeats to
          send as we are reading

          if reading fails like client disconnect this throws
          boost::system::system_error
        */

        std::size_t length = co_await boost::asio::async_read_until(
            socket_, boost::asio::dynamic_buffer(data, max_line_length), '\n');

        /*
          step 2:
          aquire lock, pushes empty token into buffer channel
          if buffer is empty -> acquire lock
          if buffer is full -> suspend this coroutine until lock is released
        */
        /*
                  no completion token pass -> defaults to use_awaitable token
        */
        // In mutual_exclusion_2:
        if (!write_lock_.try_send()) {
          co_await write_lock_.async_send();
        }
        /*
          write header and echoed line atomically for perfomances
          and "<line>" is store in compile tmie static memory(.rodata)
          sending it cost zero heap allocation
        */
        // zero copy string literal
        co_await boost::asio::async_write(socket_, "<line>"_buf);

        // echoed data
        co_await boost::asio::async_write(
            socket_, boost::asio::dynamic_buffer(data, length));

        /*
          try_receive is a synchronous non-blocking receive  pop the token from
          the buffer if token exist return true else false
        */

        write_lock_.try_receive([](auto &&...) {});
      }
    } catch (std::exception &) {
      stop();
    }
  }

  auto send_heartbeats() -> boost::asio::awaitable<void> {
    using namespace std::chrono_literals;
    using namespace boost::asio::buffer_literals;
    try {
      boost::asio::steady_timer timer_{socket_.get_executor()};

      while (true) {
        // wait for 1sec async
        timer_.expires_after(1s);
        co_await timer_.async_wait();

        // In mutual_exclusion_2:
        if (!write_lock_
                 .try_send()) { // this pushes notification synchronously if not
                                // possible use co_await optimisinistic locking
          co_await write_lock_.async_send();
        }

        /*
          send static string to socket
        */
        co_await boost::asio::async_write(socket_, "<heartbeat>\n"_buf);

        // release lock
        write_lock_.try_receive([](auto...) {});
      }
    } catch (std::exception &) {
      stop();
    }
  }
};

auto listen(boost::asio::ip::tcp::acceptor &acceptor)
    -> boost::asio::awaitable<void> {
  while (true) {
    /*
      completion token : as_tuple it change completion signature from
     void(error_code, tcp::socket) to std::tuple<error_code, tcp::socket>
     avoid throwing exception
    */

    auto [e, socket] = co_await acceptor.async_accept(boost::asio::as_tuple);

    if (!e) {
      std::make_shared<line_based_echo_session>(std::move(socket))->start();
    }
  }
}

int start_example_1(boost::asio::io_context &ctx, int argc, char *argv[]) {
  try {

    if (argc != 3) {
      std::cerr
          << "Usage: mutual_exclusion_server <listen_address> <listen_port>\n";
      std::cerr << "Example: ./mutual_exclusion_server 0.0.0.0 5555\n";
      return 1;
    }

    using boost::asio::ip::tcp;
    auto listen_endpoint =
        *tcp::resolver(ctx)
             .resolve(argv[1], argv[2], tcp::resolver::passive)
             .begin();

    tcp::acceptor acceptor(ctx, listen_endpoint);

    boost::asio::co_spawn(ctx, listen(acceptor), boost::asio::detached);

    std::cout << "Server listening on " << argv[1] << ":" << argv[2] << "...\n";

    ctx.run();

  } catch (const std::exception &e) {

    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
} // namespace example_1
