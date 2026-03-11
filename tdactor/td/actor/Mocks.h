/*
 * Copyright (c) 2026, TON CORE TECHNOLOGIES CO. L.L.C
 *
 * SPDX-License-Identifier: LGPL-2.0-or-later
 */

#pragma once

#include <deque>
#include <memory>
#include <tuple>
#include <variant>
#include <vector>

#include "td/actor/BusRuntime.h"
#include "td/utils/type_traits.h"

namespace td::actor {

// =============================================================================
// MockAsync<R, Args...> — dequeue-based mock for coroutine RPCs
// =============================================================================

template <typename R, typename... Args>
class MockAsync {
 public:
  using CallArgs = std::tuple<std::decay_t<Args>...>;

  struct PendingCall {
    CallArgs args;
    Promise<R> respond;
  };

  void returns(R result) {
    results_.push_back(std::move(result));
  }

  StartedTask<PendingCall> expect() {
    CHECK(!has_pending_expect_);
    auto [task, promise] = StartedTask<PendingCall>::make_bridge();
    pending_expect_ = std::move(promise);
    has_pending_expect_ = true;
    return std::move(task);
  }

  Task<R> call(Args... args) {
    call_count_++;
    if (has_pending_expect_) {
      has_pending_expect_ = false;
      auto [result_task, result_promise] = StartedTask<R>::make_bridge();
      auto pe = std::exchange(pending_expect_, {});
      pe.set_value(PendingCall{CallArgs(std::move(args)...), std::move(result_promise)});
      co_return co_await std::move(result_task);
    }
    CHECK(!results_.empty());
    auto result = std::move(results_.front());
    results_.pop_front();
    co_return std::move(result);
  }

  int call_count() const {
    return call_count_;
  }

 private:
  std::deque<R> results_;
  Promise<PendingCall> pending_expect_;
  bool has_pending_expect_ = false;
  int call_count_ = 0;
};

// =============================================================================
// MockBus / MockActor — generalized event-capturing test bus infrastructure
// =============================================================================

template <typename T>
concept HasReturnType = requires { typename T::ReturnType; };

namespace detail {

template <typename...>
struct RequestsHelper;

template <>
struct RequestsHelper<> {
  using type = std::tuple<>;
};

template <HasReturnType T, typename... Ts>
struct RequestsHelper<T, Ts...> {
  using type = td::TuplePrepend<T, typename RequestsHelper<Ts...>::type>;
};

template <typename T, typename... Ts>
struct RequestsHelper<T, Ts...> {
  using type = typename RequestsHelper<Ts...>::type;
};

}  // namespace detail

template <typename... Ts>
using Requests = typename detail::RequestsHelper<Ts...>::type;

template <typename, typename, typename>
struct MockActor;

template <typename B, typename... Es>
struct MockBus : B {
  using Parent = B;
  using Events = td::TypeList<>;

  using E = std::tuple<Es...>;
  using R = Requests<Es...>;

  static Runtime create_runtime() {
    td::actor::Runtime runtime;
    runtime.register_actor<MockActor<MockBus, E, R>>("Mock");
    return runtime;
  }

  mutable MockActor<MockBus, E, R>* actor;
};

template <typename B, typename... Es, typename... Rs>
struct MockActor<B, std::tuple<Es...>, std::tuple<Rs...>> : SpawnsWith<B>, ConnectsTo<B> {
  using BH = BusHandle<B>;

  std::vector<std::variant<std::shared_ptr<const Es>...>> events_;
  std::tuple<std::deque<typename Rs::ReturnType>...> results_;

  void start_up() override {
    this->owning_bus()->actor = this;
  }

  template <typename BB, typename EE>
  void handle(BusHandle<BB>, std::shared_ptr<const EE>) = delete;

  template <typename BB, typename EE>
  Task<typename EE::ReturnType> process(BusHandle<BB>, std::shared_ptr<const EE>) = delete;

  template <std::same_as<B>, td::OneOf<Es...> E>
  void handle(BH, std::shared_ptr<const E> event) {
    events_.emplace_back(std::move(event));
  }

  template <std::same_as<B>, td::OneOf<Rs...> R>
  Task<typename R::ReturnType> process(BH, std::shared_ptr<R> request) {
    events_.emplace_back(std::move(request));
    std::deque<typename R::ReturnType>& results = std::get<td::IndexOf<R, Rs...>>(results_);
    CHECK(!results.empty());
    typename R::ReturnType result = std::move(results.front());
    results.pop_front();
    co_return std::move(result);
  }
};

// =============================================================================
// Event query helpers
// =============================================================================

template <typename E, typename... Es>
size_t count_events(const std::vector<std::variant<std::shared_ptr<const Es>...>>& events) {
  size_t count = 0;
  for (auto& e : events) {
    if (std::holds_alternative<std::shared_ptr<const E>>(e)) {
      ++count;
    }
  }
  return count;
}

template <typename E, typename... Es>
std::vector<std::shared_ptr<const E>> events_of(const std::vector<std::variant<std::shared_ptr<const Es>...>>& events) {
  std::vector<std::shared_ptr<const E>> result;
  for (auto& e : events) {
    if (auto* p = std::get_if<std::shared_ptr<const E>>(&e)) {
      result.push_back(*p);
    }
  }
  return result;
}

}  // namespace td::actor
