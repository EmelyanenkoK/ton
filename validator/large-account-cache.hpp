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

#include "interfaces/validator-manager.h"
#include "td/actor/actor.h"
#include "td/db/RocksDb.h"

namespace ton::validator {

class LargeAccountCache : public td::actor::Actor {
 public:
  LargeAccountCache(std::string db_path, td::uint64 size_cap_bytes, td::uint32 min_account_cells);

  void start_up() override;

  void get_access(td::Promise<LargeAccountCacheAccess> promise);
  void update(std::vector<LargeAccountCacheUpdate> updates);

 private:
  struct State {
    td::uint64 size_cap_bytes{0};
    td::uint32 min_account_cells{0};
    std::shared_ptr<td::RocksDb> kv;

    td::optional<LargeAccountCacheValue> lookup(const td::Bits256& hash) const;
  };

  std::string db_path_;
  std::shared_ptr<State> state_ = std::make_shared<State>();

  td::Status update_impl(std::vector<LargeAccountCacheUpdate> updates);
};

}  // namespace ton::validator
