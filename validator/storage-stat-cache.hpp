/*
    This file is part of TON Blockchain Library.

    TON Blockchain Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    TON Blockchain Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with TON Blockchain Library.  If not, see <http://www.gnu.org/licenses/>.
*/
#pragma once
#include <functional>
#include <memory>

#include "interfaces/validator-manager.h"
#include "td/utils/LRUCache.h"
#include "td/utils/port/RwMutex.h"

namespace ton::validator {

class StorageStatCache : public td::actor::Actor {
 public:
  void get_cache(td::Promise<std::function<td::Ref<vm::Cell>(const td::Bits256&)>> promise);

  // (storage dict root, account total cells)
 void update(std::vector<std::pair<td::Ref<vm::Cell>, td::uint32>> data);

 private:
  struct State {
    td::Ref<vm::Cell> lookup(const td::Bits256& hash) {
      auto guard = rw_mutex_.lock_read().move_as_ok();
      return cache_.lookup_ref(hash);
    }

    void set(const td::Bits256& hash, td::Ref<vm::Cell> cell) {
      auto guard = rw_mutex_.lock_write().move_as_ok();
      cache_.set_ref(hash, std::move(cell));
    }

    bool erase(const td::Bits256& hash) {
      auto guard = rw_mutex_.lock_write().move_as_ok();
      return cache_.lookup_delete_ref(hash).not_null();
    }

   private:
    td::RwMutex rw_mutex_;
    vm::Dictionary cache_{256};
  };

  struct Deleter {
    Deleter(const td::Bits256& hash, std::shared_ptr<State> state) : hash(hash), state(std::move(state)) {
    }
    Deleter(const Deleter&) = delete;
    Deleter(Deleter&& other) noexcept : hash(other.hash), state(std::move(other.state)) {
    }
    Deleter& operator=(const Deleter&) = delete;
    Deleter& operator=(Deleter&& other) noexcept {
      hash = other.hash;
      state = std::move(other.state);
      return *this;
    }
    ~Deleter() {
      if (state) {
        CHECK(state->erase(hash));
        LOG(DEBUG) << "StorageStatCache remove " << hash.to_hex();
      }
    }

    td::Bits256 hash = td::Bits256::zero();
    std::shared_ptr<State> state;
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
  td::LRUCache<td::Bits256, Deleter> lru_{MAX_CACHE_TOTAL_CELLS};

  static constexpr td::uint64 MAX_CACHE_TOTAL_CELLS = 1 << 24;

 public:
  static constexpr td::uint64 MIN_ACCOUNT_CELLS = 4000;
};

}  // namespace ton::validator
