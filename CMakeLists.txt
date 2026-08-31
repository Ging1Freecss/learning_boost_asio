cmake_minimum_required(VERSION 3.25)
project(learning_asio LANGUAGES CXX)

# ── C++23 ────────────────────────────────────────────────────────────────────
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Dependencies (resolved via vcpkg) ───────────────────────────────────────
find_package(Boost REQUIRED COMPONENTS system)

# ── Target ───────────────────────────────────────────────────────────────────
add_executable(${PROJECT_NAME}
  src/main.cpp
  src/timer_sequence.cpp
  include/basic_boost.hpp
  src/basic_boost.cpp
  include/coroutine_boost.hpp
  src/coroutine_boost.cpp
)

target_include_directories(${PROJECT_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_link_libraries(${PROJECT_NAME} PRIVATE Boost::headers Boost::system)

target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra -Wpedantic)
