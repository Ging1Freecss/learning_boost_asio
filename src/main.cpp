#include "proxy_boost.hpp"
#include <boost/asio/io_context.hpp>
#include <exception>
#include <iostream>

int main(int argc, char *argv[]) {
  try {
    boost::asio::io_context io;
    return example_2::start_example_proxy(io, argc, argv);
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
}

