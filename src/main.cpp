#include "echo_server.hpp"
#include <boost/asio/io_context.hpp>
#include <exception>
#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
  try {
    unsigned short port = 8080;
    if (argc > 1) {
      port = static_cast<unsigned short>(std::stoi(argv[1]));
    }

    std::cout << "Starting Echo Server on port " << port << "...\n";
    std::cout << "Press Ctrl+C to stop.\n";

    boost::asio::io_context io;
    return echo_server::start_echo_server(io, port);
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
}


