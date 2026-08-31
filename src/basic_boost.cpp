#include "basic_boost.hpp"
#include <array>
#include <boost/asio/buffer.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/registered_buffer.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstddef>
#include <ctime>
#include <exception>
#include <iostream>
#include <memory>
#include <string>

void timer_synchronous(executor_type ex) {
  std::cout << "Starting Timer Synchronous" << std::endl;
  boost::asio::steady_timer t(ex, boost::asio::chrono::seconds(3));
  t.wait();
  std::cout << "Stopping Timer Synchronous" << std::endl;
}

void timer_asynchronous(executor_type ex) {
  std::cout << "Starting Timer Asynchronous" << std::endl;
  auto t = std::make_shared<boost::asio::steady_timer>(
      ex, boost::asio::chrono::seconds(3));
  t->async_wait([t](const boost::system::error_code &) {
    std::cout << "Ending Timer Asynchronous" << std::endl;
  });
}

void time_ticker_print(const boost::system::error_code & /*e*/,
                       std::shared_ptr<boost::asio::steady_timer> t,
                       std::shared_ptr<int> count) {
  if (*count < 5) {
    std::cout << "Count : " << *count << std::endl;
    (*count)++;
    t->expires_at(t->expiry() + boost::asio::chrono::seconds(1));
    t->async_wait(std::bind(time_ticker_print, boost::asio::placeholders::error,
                            t, count));
  } else {
    std::cout << "Count : " << *count << std::endl;
    std::cout << "Closing time ticker" << std::endl;
  }
}
void time_ticker(executor_type ex) {
  std::cout << "starting time ticker" << std::endl;
  auto count = std::make_shared<int>(int{0});
  auto t = std::make_shared<boost::asio::steady_timer>(
      ex, boost::asio::chrono::seconds(1));
  t->async_wait(
      std::bind(time_ticker_print, boost::asio::placeholders::error, t, count));
}

int day_sync_tcp_client(boost::asio::io_context &io, int argc, char *argv[]) {
  using boost::asio::ip::tcp;
  try {
    if (argc != 2) {
      std::cerr << "Usage: client <host>" << std::endl;
      return 1;
    }
    tcp::resolver resolver(io);
    tcp::resolver::results_type endpoints =
        resolver.resolve(argv[1], "daytime");

    tcp::socket socket(io);
    boost::asio::connect(socket, endpoints);

    while (true) {
      std::array<char, 128> buf;
      boost::system::error_code error;

      size_t len = socket.read_some(boost::asio::buffer(buf), error);

      if (error == boost::asio::error::eof) {
        break;
      } else if (error) {
        throw boost::system::system_error(error);
      }

      std::cout.write(buf.data(), len);
    }
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
  return 0;
}

int day_sync_tcp_server(boost::asio::io_context &io_context, int argc,
                        char *argv[]) {
  using boost::asio::ip::tcp;
  try {
    unsigned short port = 1313;
    if (argc == 2) {
      port = static_cast<unsigned short>(std::stoi(argv[1]));
    }

    auto make_daytime_string = []() -> std::string {
      std::time_t now = std::time(0);
      return std::ctime(&now);
    };

    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), port));
    std::cout << "Daytime server running on port " << port << "..."
              << std::endl;
    while (true) {
      tcp::socket socket{io_context};
      acceptor.accept(socket);

      std::cout << "Client connected from: " << socket.remote_endpoint()
                << std::endl;

      std::string message{make_daytime_string()};

      boost::system::error_code ignored_error;
      boost::asio::write(socket, boost::asio::buffer(message), ignored_error);
    }

  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
  }
  return 0;
}

int day_sync_udp_client(boost::asio::io_context &io, int argc, char *argv[]) {
  using boost::asio::ip::udp;
  try {
    if (argc < 2) {
      std::cerr << "Usage: client <host> [port]" << std::endl;
      return 1;
    }
    const std::string port = (argc >= 3) ? argv[2] : "1313";

    udp::resolver resolver_{io};
    udp::endpoint receiver_endpoint{
        *resolver_.resolve(udp::v4(), argv[1], port).begin()};

    udp::socket socket_{io};
    socket_.open(udp::v4());

    std::array<char, 1> send_buf{{0}};
    socket_.send_to(boost::asio::buffer(send_buf), receiver_endpoint);

    std::array<char, 128> recv_buf;
    udp::endpoint sender_endpoint;
    std::size_t len =
        socket_.receive_from(boost::asio::buffer(recv_buf), sender_endpoint);

    std::cout.write(recv_buf.data(), len);
  } catch (std::exception &e) {
    std::cout << e.what() << std::endl;
  }
  return 0;
}

int day_sync_udp_server(boost::asio::io_context &io, int argc, char *argv[]) {
  using boost::asio::ip::udp;
  try {
    auto make_daytime_string = []() -> std::string {
      std::time_t now = std::time(0);
      return std::ctime(&now);
    };

    unsigned short port = 1313;
    if (argc == 2) {
      port = static_cast<unsigned short>(std::stoi(argv[1]));
    }
    udp::socket socket_(io, udp::endpoint(udp::v4(), port));
    std::cout << "UDP Daytime server listening on port " << port << "..."
              << std::endl;

    while (true) {
      std::array<char, 1> recv_buf;
      udp::endpoint remote_endpoint;

      socket_.receive_from(boost::asio::buffer(recv_buf), remote_endpoint);
      std::cout << "Received request from: " << remote_endpoint << std::endl;

      std::string message{make_daytime_string()};

      boost::system::error_code ignored_error;
      socket_.send_to(boost::asio::buffer(message), remote_endpoint, 0,
                      ignored_error);
    }
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  return 0;
}