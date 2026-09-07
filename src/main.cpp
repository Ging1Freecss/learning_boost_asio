#include "compose.hpp"
#include <exception>
#include <iostream>

int main() {
  try {
    std::cout << "Starting compose demo on port 55555...\n";
    compose::start_compose();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << "\n";
    return 1;
  }
}


