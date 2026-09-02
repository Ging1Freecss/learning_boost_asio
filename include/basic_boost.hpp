#pragma once

#include <array>
#include <boost/asio.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>

using executor_type = boost::asio::any_io_executor;
void timer_synchronous(executor_type ex);
void timer_asynchronous(executor_type ex);
void time_ticker(executor_type ex);

class time_ticker_print_class {
private:
  int count_{0};
  boost::asio::steady_timer t_;

public:
  time_ticker_print_class(executor_type ex) : count_(0), t_{ex} {
    t_.expires_after(boost::asio::chrono::seconds(1));
    t_.async_wait(std::bind(&time_ticker_print_class::print, this,
                            boost::asio::placeholders::error));
  }

  void print(const boost::system::error_code & /*e*/) {
    if (count_ < 5) {
      std::cout << "Current Count :" << count_ << std::endl;
      count_++;
      t_.expires_at(t_.expiry() + boost::asio::chrono::seconds(1));
      t_.async_wait(std::bind(&time_ticker_print_class::print, this,
                              boost::asio::placeholders::error));
    } else {
      std::cout << "Current Count :" << count_ << std::endl;
    }
  }
};

class timer_strand_print {
private:
  boost::asio::strand<boost::asio::io_context::executor_type> strand_;
  boost::asio::steady_timer timer1_;
  boost::asio::steady_timer timer2_;
  int count_{0};

public:
  timer_strand_print(boost::asio::io_context &io)
      : strand_{io.get_executor()},
        timer1_(io, boost::asio::chrono::seconds(1)),
        timer2_(io, boost::asio::chrono::seconds(1)), count_(0) {
    timer1_.async_wait(boost::asio::bind_executor(
        strand_, std::bind(&timer_strand_print::print1, this)));
    timer2_.async_wait(boost::asio::bind_executor(
        strand_, std::bind(&timer_strand_print::print2, this)));
  }

  void print1() {
    if (count_ < 10) {
      std::cout << "Timer 1 :" << count_ << std::endl;
      count_++;
      timer1_.expires_at(timer1_.expiry() + boost::asio::chrono::seconds(1));
      timer1_.async_wait(boost::asio::bind_executor(
          strand_, std::bind(&timer_strand_print::print1, this)));
    }
  }
  void print2() {
    if (count_ < 10) {
      std::cout << "Timer 2 :" << count_ << std::endl;
      count_++;
      timer2_.expires_at(timer2_.expiry() + boost::asio::chrono::seconds(1));
      timer2_.async_wait(boost::asio::bind_executor(
          strand_, std::bind(&timer_strand_print::print2, this)));
    }
  }

  ~timer_strand_print() { std::cout << "Final Count: " << count_ << std::endl; }
};

int day_sync_tcp_client(boost::asio::io_context &io, int argc, char *argv[]);

int day_sync_tcp_server(boost::asio::io_context &io_context, int argc,
                        char *argv[]);

using boost::asio::ip::tcp;
class tcp_connection : public std::enable_shared_from_this<tcp_connection> {
public:
  using pointer = std::shared_ptr<tcp_connection>;

  static auto create(boost::asio::io_context &io) -> pointer {
    return pointer(new tcp_connection{io});
  }

  auto socket() -> tcp::socket & { return socket_; }

  void start() {
    boost::system::error_code ec;
    auto remote_ep = socket_.remote_endpoint(ec);
    if (!ec) {
      std::cout << "[tcp_server] Accepted connection from: " << remote_ep
                << std::endl;
    }

    message_ = make_daytime_string();

    auto self = shared_from_this();
    boost::asio::async_write(socket_, boost::asio::buffer(message_),
                             [self](const boost::system::error_code &error,
                                    size_t bytes_transferred) {
                               self->handle_write(error, bytes_transferred);
                             });
  }

private:
  static auto make_daytime_string() -> std::string {
    std::time_t now = std::time(nullptr);
    char buf[64];
    if (auto *tm_info = std::localtime(&now)) {
      std::strftime(buf, sizeof(buf), "%a %b %d %H:%M:%S %Y\n", tm_info);
      return buf;
    }
    return "";
  }

  tcp_connection(boost::asio::io_context &io) : socket_{io} {}

  void handle_write(const boost::system::error_code &error,
                    size_t bytes_transferred) {
    if (error) {
      std::cerr << "[tcp_server] Error during write: " << error.message()
                << std::endl;
    } else {
      std::cout << "[tcp_server] Successfully sent " << bytes_transferred
                << " bytes to client." << std::endl;
    }
  }

  tcp::socket socket_;
  std::string message_;
};

class tcp_server {
public:
  tcp_server(boost::asio::io_context &io, unsigned short port = 1313)
      : io_{io}, acceptor_(io, tcp::endpoint(tcp::v4(), port)) {
    std::cout << "[tcp_server] Listening on port " << port << "..."
              << std::endl;
    start_accept();
  }

private:
  void start_accept() {
    tcp_connection::pointer new_connection = tcp_connection::create(io_);

    acceptor_.async_accept(
        new_connection->socket(),
        [this, new_connection](const boost::system::error_code &error) {
          handle_accept(new_connection, error);
        });
  }

  void handle_accept(tcp_connection::pointer new_connection,
                     const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }

    if (!error) {
      new_connection->start();
    } else {
      std::cerr << "[tcp_server] Accept error: " << error.message()
                << std::endl;
    }
    start_accept();
  }

  boost::asio::io_context &io_;
  tcp::acceptor acceptor_;
};

/*
  ---------  UDP ------------------
*/
int day_sync_udp_client(boost::asio::io_context &io, int argc, char *argv[]);
int day_sync_udp_server(boost::asio::io_context &io, int argc, char *argv[]);

using boost::asio::ip::udp;
class udp_server {
public:
  explicit udp_server(boost::asio::io_context &io, unsigned short port = 1313)
      : socket_(io, udp::endpoint(udp::v4(), port)) {
    std::cout << "[udp_server] Daytime server listening on port " << port
              << "..." << std::endl;
    start_receive();
  }

private:
  void start_receive() {
    socket_.async_receive_from(boost::asio::buffer(recv_buffer_),
                               remote_endpoint_,
                               [this](const boost::system::error_code &error,
                                      std::size_t bytes_transferred) {
                                 handle_receive(error, bytes_transferred);
                               });
  }

  void handle_receive(const boost::system::error_code &error,
                      std::size_t bytes_transferred) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }

    if (!error || error == boost::asio::error::message_size) {
      std::cout << "[udp_server] Received request (" << bytes_transferred
                << " bytes) from: " << remote_endpoint_ << std::endl;

      auto message = std::make_shared<std::string>(make_daytime_string());
      // Copy endpoint locally to prevent data race with subsequent receive
      // operations
      udp::endpoint target_endpoint = remote_endpoint_;

      socket_.async_send_to(
          boost::asio::buffer(*message), target_endpoint,
          [this, message](const boost::system::error_code &send_error,
                          std::size_t bytes_sent) {
            handle_send(send_error, bytes_sent);
          });

      start_receive();
    } else {
      std::cerr << "[udp_server] Receive error: " << error.message()
                << std::endl;
      start_receive();
    }
  }

  void handle_send(const boost::system::error_code &error,
                   std::size_t bytes_sent) {
    if (error) {
      std::cerr << "[udp_server] Send error: " << error.message() << std::endl;
    } else {
      std::cout << "[udp_server] Sent " << bytes_sent << " bytes to client."
                << std::endl;
    }
  }

  static auto make_daytime_string() -> std::string {
    std::time_t now = std::time(nullptr);
    char buf[64];
    if (auto *tm_info = std::localtime(&now)) {
      std::strftime(buf, sizeof(buf), "%a %b %d %H:%M:%S %Y\n", tm_info);
      return buf;
    }
    return "";
  }

  udp::socket socket_;
  udp::endpoint remote_endpoint_;
  std::array<char, 128> recv_buffer_{};
};

namespace both {

using boost::asio::ip::tcp;
using boost::asio::ip::udp;

class tcp_connection : public std::enable_shared_from_this<tcp_connection> {
private:
  tcp::socket socket_;
  std::string message_;

public:
  using pointer = std::shared_ptr<tcp_connection>;

  static auto create(boost::asio::io_context &io) -> pointer {
    return pointer(new tcp_connection(io));
  }

  auto socket() -> tcp::socket & { return socket_; }

  void start() {
    message_ = make_daytime_string();
    auto self = shared_from_this();
    boost::asio::async_write(socket_, boost::asio::buffer(message_),
                             [self](const boost::system::error_code &send_error,
                                    std::size_t bytes_sent) {
                               if (send_error) {
                                 std::cerr << "[both::tcp_server] Send error: "
                                           << send_error.message() << std::endl;
                               } else {
                                 std::cout << "[both::tcp_server] Sent "
                                           << bytes_sent << " bytes."
                                           << std::endl;
                               }
                             });
  }

private:
  tcp_connection(boost::asio::io_context &io_context) : socket_(io_context) {}

  static auto make_daytime_string() -> std::string {
    std::time_t now = std::time(nullptr);
    char buf[64];
    if (auto *tm_info = std::localtime(&now)) {
      std::strftime(buf, sizeof(buf), "%a %b %d %H:%M:%S %Y\n", tm_info);
      return buf;
    }
    return "";
  }
};

class tcp_server {
private:
  boost::asio::io_context &io_;
  tcp::acceptor acceptor_;

public:
  tcp_server(boost::asio::io_context &io, unsigned short port = 1313)
      : io_(io), acceptor_(io, tcp::endpoint(tcp::v4(), port)) {
    std::cout << "[both::tcp_server] Listening on port " << port << "..."
              << std::endl;
    start_accept();
  }

private:
  void start_accept() {
    tcp_connection::pointer new_connection = tcp_connection::create(io_);

    acceptor_.async_accept(
        new_connection->socket(),
        [this, new_connection](const boost::system::error_code &error) {
          handle_accept(new_connection, error);
        });
  }

  void handle_accept(tcp_connection::pointer new_connection,
                     const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }

    if (!error) {
      new_connection->start();
    } else {
      std::cerr << "[both::tcp_server] Accept error: " << error.message()
                << std::endl;
    }

    start_accept();
  }
};

class udp_server {
private:
  udp::socket socket_;
  udp::endpoint remote_endpoint_;
  std::array<char, 128> recv_buffer_{};

public:
  udp_server(boost::asio::io_context &io_context, unsigned short port = 1313)
      : socket_(io_context, udp::endpoint(udp::v4(), port)) {
    std::cout << "[both::udp_server] Daytime server listening on port " << port
              << "..." << std::endl;
    start_receive();
  }

  void start_receive() {
    socket_.async_receive_from(boost::asio::buffer(recv_buffer_),
                               remote_endpoint_,
                               [this](const boost::system::error_code &error,
                                      std::size_t bytes_transferred) {
                                 this->handle_receive(error, bytes_transferred);
                               });
  }

  void handle_receive(const boost::system::error_code &error,
                      std::size_t bytes_transferred) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }

    if (!error || error == boost::asio::error::message_size) {
      std::cout << "[both::udp_server] Received request (" << bytes_transferred
                << " bytes) from: " << remote_endpoint_ << std::endl;

      auto message = std::make_shared<std::string>(make_daytime_string());
      udp::endpoint target_endpoint = remote_endpoint_;

      socket_.async_send_to(
          boost::asio::buffer(*message), target_endpoint,
          [this, message](const boost::system::error_code &send_error,
                          std::size_t bytes_sent) {
            handle_send(send_error, bytes_sent);
          });

      start_receive();
    } else {
      std::cerr << "[both::udp_server] Receive error: " << error.message()
                << std::endl;
      start_receive();
    }
  }

  void handle_send(const boost::system::error_code &error,
                   std::size_t bytes_sent) {
    if (error) {
      std::cerr << "[both::udp_server] Send error: " << error.message()
                << std::endl;
    } else {
      std::cout << "[both::udp_server] Sent " << bytes_sent
                << " bytes to client." << std::endl;
    }
  }

private:
  static auto make_daytime_string() -> std::string {
    std::time_t now = std::time(nullptr);
    char buf[64];
    if (auto *tm_info = std::localtime(&now)) {
      std::strftime(buf, sizeof(buf), "%a %b %d %H:%M:%S %Y\n", tm_info);
      return buf;
    }
    return "";
  }
};
} // namespace both
