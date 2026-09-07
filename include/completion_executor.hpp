
#include <boost/asio.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/steady_timer.hpp>
#include <concepts>
#include <iostream>

/*
    when a async operation finishes it invokes its completion
    handler, by default it run on the I/O context that submitted it.

    by binding a custom completion executor, you can intercept or redirect
   handler execution
*/
namespace completion_executor {
using boost::asio::ip::tcp;
/*
    every executor bound via 'bind_executor' must satisfy
    1. equality overload marked noexcept
    2. const qualified 'execute member function' taking an invocable target
    3. copy/move semantics
*/
class logging_executor {
public:
  /*
      differentiate between two executors if equal optimise away unnecessary
      context switching or re-marshaling
      logging_executor is stateless so default equality is sufficient
  */
  bool operator==(const logging_executor &) const noexcept = default;

  void execute(std::invocable auto handler) const {
    try {
      std::cout << "[logging_executor] >>> Handler invocation starting\n";

      // Move and invoke the completion handler.
      std::move(handler)();

      std::cout << "[logging_executor] <<< Handler invocation complete\n";
    } catch (...) {
      // If the completion handler throws, we log the failure and rethrow.
      std::cout << "[logging_executor] !!! Handler invocation completed with "
                   "exception\n";
      throw;
    }
  }
};

// 1. Verify that `logging_executor` satisfies Boost.Asio's `executor` concept.
// This checks at compile time that execute(), copy constructors, and equality
// operators exist and adhere to Asio's requirements.
static_assert(
    boost::asio::execution::executor<logging_executor>,
    "logging_executor must satisfy boost::asio::execution::executor concept");
// 2. Verify that `logging_executor` can be implicitly converted into Asio's
// polymorphic type-erased completion executor
// (`boost::asio::any_completion_executor`). This ensures it can be passed to
// any asynchronous operation or completion token.
static_assert(
    std::convertible_to<logging_executor, boost::asio::any_completion_executor>,
    "logging_executor must be convertible to any_completion_executor");

int start_completion_executor(boost::asio::io_context &io_context) {
  boost::asio::post(io_context,
                    boost::asio::bind_executor(logging_executor{}, [] {
                      std::cout << "  -> [post] Handler executed\n";
                    }));

  tcp::endpoint endpoint(boost::asio::ip::address_v4::loopback(), 0);
  tcp::acceptor acceptor(io_context, endpoint);
  tcp::socket server_socket(io_context);

  acceptor.async_accept(
      boost::asio::bind_executor(logging_executor{}, [](auto...) {
        std::cout << "  -> [async_accept] Handler executed\n";
      }));
  tcp::socket client_socket(io_context);
  client_socket.connect(acceptor.local_endpoint());

  boost::asio::steady_timer timer(io_context);
  timer.expires_at(boost::asio::steady_timer::clock_type::time_point::min());

  timer.async_wait(boost::asio::bind_executor(logging_executor{}, [](auto...) {
    std::cout << "  -> [async_wait] Handler executed\n";
  }));

  std::cout << "--- Starting io_context.run() ---\n";
  io_context.run();
  std::cout << "--- Finished io_context.run() ---\n";
  return 0;
}
} // namespace completion_executor