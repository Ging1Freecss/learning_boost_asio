#pragma once

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <exception>
#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <utility>
// Boost.Asio C++20 Coroutine headers
#include <boost/asio/awaitable.hpp> // Provides boost::asio::awaitable<T> return type for coroutines
#include <boost/asio/co_spawn.hpp> // Spawns a coroutine as an asynchronous task on an executor
#include <boost/asio/detached.hpp> // Completion token indicating fire-and-forget (ignore result/exceptions)
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp> // Core I/O event loop
#include <boost/asio/ip/tcp.hpp> // TCP socket, acceptor, and endpoint classes
#include <boost/asio/read_until.hpp> // Asynchronously reads until a delimiter (e.g. "\n")
#include <boost/asio/redirect_error.hpp> // Captures error_code without throwing an exception
#include <boost/asio/signal_set.hpp> // Handles POSIX signals like SIGINT and SIGTERM
#include <boost/asio/steady_timer.hpp> // Async timer (used here as an async condition variable)
#include <boost/asio/use_awaitable.hpp> // Completion token that makes async functions return an awaitable
#include <boost/asio/write.hpp> // Asynchronously writes all data to a stream

namespace chat_server {

using boost::asio::ip::tcp;

class chat_participant {
public:
  virtual ~chat_participant() = default;

  virtual void deliver(const std::string &msg) = 0;
};

using chat_participant_ptr = std::shared_ptr<chat_participant>;

/*
    a single instance of chat room exists per chat service
    coordinates between all participants and maintains recent message history
*/
class chat_room {
public:
  void join(chat_participant_ptr participant) {
    participants_.insert(participant);

    // new join participant, relay the last 100 messages
    for (const auto &msg : recent_msgs_) {
      participant->deliver(msg);
    }
  }

  void leave(chat_participant_ptr participant) {
    participants_.erase(participant);
  }

  // broadcast all message to all the participants in the room
  void deliver(const std::string &msg) {
    recent_msgs_.push_back(msg);

    while (recent_msgs_.size() > max_recent_msgs) {
      recent_msgs_.pop_front();
    }

    /*
        loop through participants and push msg into their write queue
        without blocking
    */
    for (auto &participant : participants_) {
      participant->deliver(msg);
    }
  }

private:
  std::set<chat_participant_ptr> participants_;

  constexpr static std::size_t max_recent_msgs{100};
  std::deque<std::string> recent_msgs_;
};

/*
    one instance of chat session per connected client
*/
class chat_session : public chat_participant,
                     public std::enable_shared_from_this<chat_session> {
public:
  chat_session(tcp::socket socket, std::shared_ptr<chat_room> room)
      : socket_{std::move(socket)}, room_(std::move(room)),
        write_channel_(socket_.get_executor(), max_write_msgs) {}

  void start() {
    room_->join(shared_from_this());

    boost::asio::co_spawn(
        socket_.get_executor(),
        [self = shared_from_this()] { return self->reader(); },
        boost::asio::detached);

    boost::asio::co_spawn(
        socket_.get_executor(),
        [self = shared_from_this()] { return self->writer(); },
        boost::asio::detached);
  }

  void deliver(const std::string &msg) {
    if (stopped_)
      return;
    if (!write_channel_.try_send(boost::system::error_code{}, msg)) {
      // channel is full , evict the last one
      write_channel_.try_receive([](auto...) {});

      // push the latest msg
      write_channel_.try_send(boost::system::error_code{}, msg);
    }
  }

private:
  auto reader() -> boost::asio::awaitable<void> {
    try {
      std::string read_msg{};
      while (true) {
        std::size_t n = co_await boost::asio::async_read_until(
            socket_, boost::asio::dynamic_buffer(read_msg, max_msg_bytes), "\n",
            boost::asio::use_awaitable);

        room_->deliver(read_msg.substr(0, n));
        read_msg.erase(0, n);
      }
    } catch (const std::exception &) {
      stop();
    }
  }

  auto writer() -> boost::asio::awaitable<void> {
    try {
      while (socket_.is_open()) {

        auto [e, msg] = co_await write_channel_.async_receive(
            boost::asio::as_tuple(boost::asio::use_awaitable));

        if (e == boost::asio::experimental::error::channel_closed) {
          break;
        }
        // 2. Unexpected error: throw it so the catch block handles it
        if (e) {
          throw boost::system::system_error(e);
        }
        co_await boost::asio::async_write(socket_, boost::asio::buffer(msg),
                                          boost::asio::use_awaitable);
      }

    } catch (const std::exception &) {
      stop();
    }
  }

  void stop() {
    if (stopped_) {
      return;
    }
    stopped_ = true;
    room_->leave(shared_from_this());
    boost::system::error_code ec;
    write_channel_.close();
    auto close_ec = socket_.close(ec);
    (void)close_ec;
  }

  constexpr static std::size_t max_write_msgs{100};
  constexpr static std::size_t max_msg_bytes{4096};
  tcp::socket socket_;
  std::shared_ptr<chat_room> room_;
  boost::asio::experimental::channel<void(boost::system::error_code,
                                          std::string)>
      write_channel_;
  bool stopped_{false};
};

auto listener(tcp::acceptor acceptor_, std::shared_ptr<chat_room> room)
    -> boost::asio::awaitable<void> {
  while (acceptor_.is_open()) {
    try {
      auto socket = co_await acceptor_.async_accept(boost::asio::use_awaitable);
      std::make_shared<chat_session>(std::move(socket), room)->start();
    } catch (const boost::system::system_error &e) {
      if (e.code() == boost::asio::error::operation_aborted) {
        break;
      }
      std::cerr << "Listener accept error: " << e.what() << "\n";
    } catch (const std::exception &e) {
      std::cerr << "Listener accept error: " << e.what() << "\n";
    }
  }
}

auto chat_server(boost::asio::io_context &io_context, int argc, char *argv[])
    -> int {
  try {
    auto room = std::make_shared<chat_room>();

    if (argc < 2) {
      unsigned short port = 8080;
      std::cout << "Starting Chat Server on port " << port << "...\n";
      boost::asio::co_spawn(
          io_context,
          listener(tcp::acceptor(io_context, {tcp::v4(), port}), room),
          boost::asio::detached);
    } else {
      for (int i = 1; i < argc; i++) {
        unsigned short port = static_cast<unsigned short>(std::atoi(argv[i]));
        std::cout << "Starting Chat Server on port " << port << "...\n";
        boost::asio::co_spawn(
            io_context,
            listener(tcp::acceptor(io_context, {tcp::v4(), port}), room),
            boost::asio::detached);
      }
    }

    std::cout << "Press Ctrl+C to stop.\n";

    boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto...) { io_context.stop(); });

    io_context.run();
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
} // namespace chat_server