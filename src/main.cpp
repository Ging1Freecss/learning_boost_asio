#include "completion_executor.hpp"
#include <boost/asio/io_context.hpp>
#include <exception>
#include <iostream>

int main() {
  try {
    boost::asio::io_context io;
    return completion_executor::start_completion_executor(io);
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
}


