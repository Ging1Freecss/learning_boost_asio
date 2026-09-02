#pragma once

#include <array>
#include <boost/asio.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <iostream>
namespace example_2 {

using boost::asio::ip::tcp;

using token_channel = boost::asio::experimental::channel<void(
    boost::system::error_code, std::size_t)>;

/*
    token producing coroutine(rate limiting engine)
    uses "Token bucket" rate limiting algo

    bytes_per_token: byte quota granted per tick (here it is 20kb)
    token_interval: how often a token is minted(here it is 100ms)
    tokens: channel where tokens are deposited
*/
auto produce_tokens(std::size_t bytes_per_token,
                    boost::asio::steady_timer::duration token_interval,
                    token_channel &tokens) -> boost::asio::awaitable<void> {
  /*
    co_await boost::asio::this_coro::executor obtain the executor of the
    current coroutine frame(i.e produce_token)

    in boost asio when a awaitable<T> type co_await a childred awaitable<T> type
    in automatically pass it's executor down to child coroutine frame
  */
  boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor};

  while (true) {
    /*
      deposit a token into the channel , if buffer is full it suspend
    */

    co_await tokens.async_send(boost::system::error_code{}, bytes_per_token);

    // set the interval and block for 100ms
    timer.expires_after(token_interval);
    co_await timer.async_wait();
  }
}
/*
  pump data from socket "from" to socket "to" , while limiting flow using token
  channel
*/

auto transfer(tcp::socket &from, tcp::socket &to, token_channel &tokens)
    -> boost::asio::awaitable<void> {
  std::array<unsigned char, 4096> data; // 4kb local buffer

  while (true) {
    // wait for byte available for consumption
    std::size_t bytes_available = co_await tokens.async_receive();

    while (bytes_available > 0) {
      /*
        read minimum of min(bytes_available,data.size()) or until EOF
      */
      std::size_t n = co_await from.async_read_some(
          boost::asio::buffer(data, bytes_available));

      co_await boost::asio::async_write(to, boost::asio::buffer(data, n));

      bytes_available -= n;
    }
  }
}

/*
  manages the full lifecycle of a single client connection
  1. Connects to the backend
  2. Spawn two independent data streams (Client->server and server->client)
  3. coordinates automatic teardown and cancellatino when either side
  disconnects
*/
auto proxy(tcp::socket client, tcp::endpoint target)
    -> boost::asio::awaitable<void> {
  /*
    Throttlnig Configuration
    channel capacity = 100 tokens, max_burst = 100*20KB = 2MB
  */
  using namespace std::literals::chrono_literals;

  constexpr std::size_t number_of_tokens{100};
  constexpr std::size_t bytes_per_tokens{20 * 1024}; // 20kb
  constexpr boost::asio::steady_timer::duration token_interval{100ms};

  auto ex = client.get_executor();

  tcp::socket server{ex}; // this connect to backend server

  // bucket for upsteam and downstream traffixc
  token_channel client_channel{ex, number_of_tokens};
  token_channel server_channel{ex, number_of_tokens};

  try {
    /*
  connect to target backend server
  suspend until three 3-way tcp handshake completes
  */

    co_await server.async_connect(target);

    /*
      logical AND operator (awaitable operators)
      it waits for all coroutines to finish, if any one coroutine throw
      exceptoin or fails operator&& immediatelly cancels all other running
      coroutines and propagate the exception
    */

    using namespace boost::asio::experimental::awaitable_operators;
    co_await (
        produce_tokens(bytes_per_tokens, token_interval, client_channel) &&
        transfer(client, server, client_channel) &&
        produce_tokens(bytes_per_tokens, token_interval, server_channel) &&
        transfer(server, client, server_channel));
  } catch (const boost::system::system_error &e) {
    // Filter out normal disconnection codes
    if (e.code() != boost::asio::error::eof &&
        e.code() != boost::asio::error::operation_aborted &&
        e.code() != boost::asio::error::connection_reset &&
        e.code() != boost::asio::error::broken_pipe) {
      std::cerr << "Proxy session error: " << e.what() << "\n";
    }
  } catch (const std::exception &e) {
    std::cerr << "Unexpected exception: " << e.what() << "\n";
  }
}

auto listen(tcp::acceptor &acceptor, tcp::endpoint target)
    -> boost::asio::awaitable<void> {
  while (true) {
    auto [e, client] = co_await acceptor.async_accept(boost::asio::as_tuple);

    if (!e) {
      auto ex = client.get_executor();

      boost::asio::co_spawn(ex, proxy(std::move(client), target),
                            boost::asio::detached);
    } else {
      std::cerr << "Accept failed: " << e.message() << "\n";
      boost::asio::steady_timer timer(
          co_await boost::asio::this_coro::executor);
      using namespace std::literals::chrono_literals;
      timer.expires_after(100ms);

      co_await timer.async_wait();
    }
  }
}

int start_example_proxy(boost::asio::io_context &ctx, int argc, char *argv[]) {
  if (argc != 5) {
    std::cerr << "Usage: throttling_proxy";
    std::cerr << " <listen_address> <listen_port>";
    std::cerr << " <target_address> <target_port>\n";
    return 1;
  }
  // Resolve the listening address and port into a local TCP endpoint.
  // tcp::resolver::passive indicates this endpoint will be used with an
  // acceptor.
  auto listen_endpoint = *tcp::resolver(ctx)
                              .resolve(argv[1], argv[2], tcp::resolver::passive)
                              .begin();
  // Resolve the remote backend target host and port.
  auto target_endpoint = *tcp::resolver(ctx).resolve(argv[3], argv[4]).begin();

  tcp::acceptor acceptor(ctx, listen_endpoint);

  co_spawn(ctx, listen(acceptor, target_endpoint), boost::asio::detached);
  ctx.run();
  return 0;
}
} // namespace example_2