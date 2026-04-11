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
#include "large-account-cache.hpp"

#include <rocksdb/db.h>

#include "td/utils/misc.h"
#include "td/utils/port/path.h"
#include "td/utils/ScopeGuard.h"
#include "vm/boc.h"

namespace ton::validator {
namespace {

constexpr char LARGE_ACCOUNT_CACHE_VERSION = 1;
constexpr const char* USED_BYTES_KEY = "meta/used_bytes";
constexpr double MAX_UPDATE_QUEUE_AGE = 3.0;

std::string entry_key(const td::Bits256& hash) {
  return PSTRING() << "entry/" << hash.to_hex();
}

std::string refcnt_key(const td::Bits256& hash) {
  return PSTRING() << "refcnt/" << hash.to_hex();
}

std::string bytes_key(const td::Bits256& hash) {
  return PSTRING() << "bytes/" << hash.to_hex();
}

std::string current_key(WorkchainId workchain, const StdSmcAddress& addr) {
  return PSTRING() << "current/" << workchain << ":" << addr.to_hex();
}

void append_uint64(std::string& out, td::uint64 value) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>(value & 255));
    value >>= 8;
  }
}

td::Result<td::uint64> read_uint64(td::Slice data, size_t& offset) {
  if (offset + 8 > data.size()) {
    return td::Status::Error("truncated uint64");
  }
  td::uint64 value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<td::uint64>(static_cast<unsigned char>(data[offset + i])) << (i * 8);
  }
  offset += 8;
  return value;
}

td::Result<td::optional<std::string>> get_optional_string(td::KeyValue& kv, td::Slice key) {
  std::string value;
  TRY_RESULT(status, kv.get(key, value));
  if (status == td::KeyValue::GetStatus::NotFound) {
    return td::optional<std::string>();
  }
  return value;
}

td::Result<td::optional<td::uint64>> get_optional_uint64(td::KeyValue& kv, td::Slice key) {
  TRY_RESULT(opt_value, get_optional_string(kv, key));
  if (!opt_value) {
    return td::optional<td::uint64>();
  }
  TRY_RESULT(value, td::to_integer_safe<td::uint64>(*opt_value));
  return value;
}

td::Result<td::optional<td::Bits256>> get_optional_hash(td::KeyValue& kv, td::Slice key) {
  TRY_RESULT(opt_value, get_optional_string(kv, key));
  if (!opt_value) {
    return td::optional<td::Bits256>();
  }
  td::Bits256 hash;
  if (hash.from_hex(*opt_value) != 256) {
    return td::Status::Error("invalid hash encoding");
  }
  return hash;
}

td::Status set_uint64(td::KeyValue& kv, td::Slice key, td::uint64 value) {
  return kv.set(key, td::to_string(value));
}

td::Result<std::string> serialize_entry(const td::Ref<vm::Cell>& dict_root, const std::vector<td::Ref<vm::Cell>>& roots) {
  if (dict_root.is_null()) {
    return td::Status::Error("dict_root is null");
  }
  TRY_RESULT(dict_root_boc, vm::std_boc_serialize(dict_root));
  TRY_RESULT(roots_boc, vm::std_boc_serialize_multi(roots));
  std::string result;
  result.reserve(1 + 16 + dict_root_boc.size() + roots_boc.size());
  result.push_back(LARGE_ACCOUNT_CACHE_VERSION);
  append_uint64(result, dict_root_boc.size());
  append_uint64(result, roots_boc.size());
  result.append(dict_root_boc.as_slice().str());
  result.append(roots_boc.as_slice().str());
  return result;
}

td::Result<LargeAccountCacheValue> deserialize_entry(td::Slice data) {
  if (data.empty()) {
    return td::Status::Error("empty large-account cache entry");
  }
  if (data[0] != LARGE_ACCOUNT_CACHE_VERSION) {
    return td::Status::Error("unsupported large-account cache entry version");
  }
  size_t offset = 1;
  TRY_RESULT(dict_root_size, read_uint64(data, offset));
  TRY_RESULT(roots_size, read_uint64(data, offset));
  if (offset + dict_root_size + roots_size != data.size()) {
    return td::Status::Error("invalid large-account cache entry size");
  }
  TRY_RESULT(dict_root, vm::std_boc_deserialize(data.substr(offset, dict_root_size)));
  offset += dict_root_size;
  TRY_RESULT(roots, vm::std_boc_deserialize_multi(data.substr(offset, roots_size)));
  return LargeAccountCacheValue{std::move(dict_root), std::move(roots)};
}

}  // namespace

LargeAccountCache::LargeAccountCache(std::string db_path, td::uint64 size_cap_bytes, td::uint32 min_account_cells)
    : db_path_(std::move(db_path)) {
  state_->size_cap_bytes = size_cap_bytes;
  state_->min_account_cells = min_account_cells;
}

void LargeAccountCache::start_up() {
  if (state_->size_cap_bytes == 0) {
    return;
  }
  td::mkdir(db_path_).ensure();
  auto r_db = td::RocksDb::open(db_path_);
  if (r_db.is_error()) {
    LOG(ERROR) << "Failed to open large-account cache DB at " << db_path_ << ": " << r_db.error();
    return;
  }
  state_->write_kv = std::make_shared<td::RocksDb>(r_db.move_as_ok());
  state_->read_kv = std::make_shared<td::RocksDb>(state_->write_kv->clone());
}

td::optional<LargeAccountCacheValue> LargeAccountCache::State::lookup(const td::Bits256& hash) const {
  if (hash.is_zero() || !read_kv) {
    return td::optional<LargeAccountCacheValue>();
  }

  auto hash_hex = hash.to_hex();
  {
    std::lock_guard<std::mutex> lock(in_flight_mutex);
    auto it = in_flight_entries.find(hash_hex);
    if (it != in_flight_entries.end()) {
      return it->second;
    }
  }

  auto r_value = get_optional_string(*read_kv, entry_key(hash));
  if (r_value.is_error()) {
    LOG(INFO) << "Failed to read large-account cache entry " << hash_hex << ": " << r_value.error();
    return td::optional<LargeAccountCacheValue>();
  }
  auto opt_value = r_value.move_as_ok();
  if (!opt_value) {
    return td::optional<LargeAccountCacheValue>();
  }

  auto r_entry = deserialize_entry(*opt_value);
  if (r_entry.is_error()) {
    LOG(INFO) << "Failed to decode large-account cache entry " << hash_hex << ": " << r_entry.error();
    return td::optional<LargeAccountCacheValue>();
  }
  auto entry = r_entry.move_as_ok();
  auto loaded_hash = td::Bits256{entry.dict_root->get_hash().bits()};
  if (loaded_hash != hash) {
    LOG(INFO) << "Large-account cache entry hash mismatch: expected " << hash_hex << " got "
              << loaded_hash.to_hex();
    return td::optional<LargeAccountCacheValue>();
  }
  return entry;
}

void LargeAccountCache::get_access(td::Promise<LargeAccountCacheAccess> promise) {
  if (!state_->read_kv) {
    promise.set_value(LargeAccountCacheAccess{});
    return;
  }
  LargeAccountCacheAccess access;
  access.min_account_cells = state_->min_account_cells;
  access.lookup = [state = state_](const td::Bits256& hash) -> td::optional<LargeAccountCacheValue> {
    return state->lookup(hash);
  };
  promise.set_value(std::move(access));
}

void LargeAccountCache::update(std::vector<LargeAccountCacheUpdate> updates) {
  if (!state_->write_kv || updates.empty()) {
    return;
  }
  auto now = td::Timestamp::now();
  std::vector<LargeAccountCacheUpdate> filtered;
  filtered.reserve(updates.size());
  std::size_t stale_updates = 0;
  for (auto& update : updates) {
    if (update.enqueued_at && now - update.enqueued_at > MAX_UPDATE_QUEUE_AGE) {
      stale_updates++;
      continue;
    }
    filtered.push_back(std::move(update));
  }
  if (stale_updates != 0) {
    LOG(INFO) << "Large-account cache: skipped " << stale_updates << " stale update(s)";
  }
  if (filtered.empty()) {
    return;
  }
  {
    std::unordered_map<std::string, LargeAccountCacheValue> in_flight_entries;
    in_flight_entries.reserve(filtered.size());
    for (const auto& update : filtered) {
      if (!update.has_storage_dict_hash() || update.dict_root.is_null()) {
        continue;
      }
      in_flight_entries[update.storage_dict_hash.value().to_hex()] = LargeAccountCacheValue{update.dict_root, update.roots};
    }
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    state_->in_flight_entries = std::move(in_flight_entries);
  }
  SCOPE_EXIT {
    std::lock_guard<std::mutex> lock(state_->in_flight_mutex);
    state_->in_flight_entries.clear();
  };
  std::function<void()> before_write_test_hook;
  {
    std::lock_guard<std::mutex> lock(state_->before_write_test_hook_mutex);
    before_write_test_hook = state_->before_write_test_hook;
  }
  if (before_write_test_hook) {
    before_write_test_hook();
  }
  auto status = update_impl(std::move(filtered));
  if (status.is_error()) {
    LOG(INFO) << "Large-account cache update failed: " << status;
  }
}

td::Status LargeAccountCache::update_impl(std::vector<LargeAccountCacheUpdate> updates) {
  td::HashMap<std::string, LargeAccountCacheUpdate> deduped;
  deduped.reserve(updates.size());
  for (auto& update : updates) {
    deduped[current_key(update.workchain, update.addr)] = std::move(update);
  }

  auto& kv = *state_->write_kv;
  TRY_STATUS(kv.begin_write_batch());
  auto status = [&]() -> td::Status {
    TRY_RESULT(opt_used_bytes, get_optional_uint64(kv, td::Slice(USED_BYTES_KEY)));
    td::uint64 used_bytes = opt_used_bytes ? opt_used_bytes.value() : 0;
    td::HashMap<std::string, td::optional<td::Bits256>> current_hashes;
    td::HashMap<std::string, td::uint64> refcnt_cache;
    td::HashMap<std::string, td::uint64> entry_bytes_cache;
    td::HashMap<std::string, bool> entry_exists_cache;

    auto read_current_hash = [&](const std::string& account_key) -> td::Result<td::optional<td::Bits256>> {
      auto it = current_hashes.find(account_key);
      if (it != current_hashes.end()) {
        return it->second;
      }
      TRY_RESULT(value, get_optional_hash(kv, account_key));
      current_hashes.emplace(account_key, value);
      return value;
    };

    auto read_refcnt = [&](const td::Bits256& hash) -> td::Result<td::uint64> {
      auto hash_hex = hash.to_hex();
      auto it = refcnt_cache.find(hash_hex);
      if (it != refcnt_cache.end()) {
        return it->second;
      }
      TRY_RESULT(opt_refcnt, get_optional_uint64(kv, refcnt_key(hash)));
      td::uint64 refcnt = opt_refcnt ? opt_refcnt.value() : 0;
      refcnt_cache.emplace(std::move(hash_hex), refcnt);
      return refcnt;
    };

    auto read_entry_bytes = [&](const td::Bits256& hash) -> td::Result<td::uint64> {
      auto hash_hex = hash.to_hex();
      auto it = entry_bytes_cache.find(hash_hex);
      if (it != entry_bytes_cache.end()) {
        return it->second;
      }
      TRY_RESULT(opt_entry_bytes, get_optional_uint64(kv, bytes_key(hash)));
      td::uint64 entry_bytes = opt_entry_bytes ? opt_entry_bytes.value() : 0;
      entry_bytes_cache.emplace(std::move(hash_hex), entry_bytes);
      return entry_bytes;
    };

    auto has_entry = [&](const td::Bits256& hash) -> td::Result<bool> {
      auto hash_hex = hash.to_hex();
      auto it = entry_exists_cache.find(hash_hex);
      if (it != entry_exists_cache.end()) {
        return it->second;
      }
      TRY_RESULT(opt_entry, get_optional_string(kv, entry_key(hash)));
      bool exists = static_cast<bool>(opt_entry);
      entry_exists_cache.emplace(std::move(hash_hex), exists);
      return exists;
    };

    auto decrement_hash = [&](const td::Bits256& hash) -> td::Status {
      TRY_RESULT(refcnt, read_refcnt(hash));
      auto hash_hex = hash.to_hex();
      if (refcnt <= 1) {
        TRY_RESULT(entry_bytes, read_entry_bytes(hash));
        used_bytes = entry_bytes <= used_bytes ? used_bytes - entry_bytes : used_bytes;
        refcnt_cache[hash_hex] = 0;
        entry_bytes_cache[hash_hex] = 0;
        entry_exists_cache[hash_hex] = false;
        TRY_STATUS(kv.erase(refcnt_key(hash)));
        TRY_STATUS(kv.erase(entry_key(hash)));
        TRY_STATUS(kv.erase(bytes_key(hash)));
      } else {
        refcnt_cache[hash_hex] = refcnt - 1;
        TRY_STATUS(set_uint64(kv, refcnt_key(hash), refcnt - 1));
      }
      return td::Status::OK();
    };

    auto increment_hash = [&](const td::Bits256& hash) -> td::Status {
      TRY_RESULT(refcnt, read_refcnt(hash));
      refcnt_cache[hash.to_hex()] = refcnt + 1;
      TRY_STATUS(set_uint64(kv, refcnt_key(hash), refcnt + 1));
      return td::Status::OK();
    };

    auto maybe_store_entry = [&](const td::Bits256& hash, const LargeAccountCacheUpdate& update,
                                 bool force_overwrite) -> td::Status {
      if (update.dict_root.is_null()) {
        return td::Status::Error("missing dict_root for large-account cache write");
      }
      TRY_RESULT(existing_entry, has_entry(hash));
      if (!force_overwrite && existing_entry) {
        return td::Status::OK();
      }

      TRY_RESULT(serialized_entry, serialize_entry(update.dict_root, update.roots));
      auto new_size = static_cast<td::uint64>(serialized_entry.size());
      TRY_RESULT(existing_bytes, read_entry_bytes(hash));

      if (!force_overwrite && !existing_entry && used_bytes + new_size > state_->size_cap_bytes) {
        return td::Status::OK();
      }

      TRY_STATUS(kv.set(entry_key(hash), serialized_entry));
      TRY_STATUS(set_uint64(kv, bytes_key(hash), new_size));
      auto hash_hex = hash.to_hex();
      entry_exists_cache[hash_hex] = true;
      entry_bytes_cache[hash_hex] = new_size;
      if (new_size >= existing_bytes) {
        used_bytes += new_size - existing_bytes;
      } else {
        used_bytes -= existing_bytes - new_size;
      }
      return td::Status::OK();
    };

    for (auto& [account_key, update] : deduped) {
      td::optional<td::Bits256> new_hash;
      if (update.has_storage_dict_hash()) {
        new_hash = update.storage_dict_hash.value();
      }

      TRY_RESULT(old_hash, read_current_hash(account_key));
      if (old_hash && new_hash && old_hash.value() == new_hash.value()) {
        current_hashes[account_key] = new_hash;
        TRY_STATUS(kv.set(account_key, new_hash.value().to_hex()));
        TRY_STATUS(maybe_store_entry(new_hash.value(), update, true));
        continue;
      }

      if (old_hash) {
        TRY_STATUS(decrement_hash(old_hash.value()));
      }

      if (new_hash) {
        current_hashes[account_key] = new_hash;
        TRY_STATUS(kv.set(account_key, new_hash.value().to_hex()));
        TRY_STATUS(increment_hash(new_hash.value()));
        TRY_STATUS(maybe_store_entry(new_hash.value(), update, false));
      } else {
        current_hashes[account_key] = td::optional<td::Bits256>();
        TRY_STATUS(kv.erase(account_key));
      }
    }

    return set_uint64(kv, td::Slice(USED_BYTES_KEY), used_bytes);
  }();

  if (status.is_error()) {
    auto abort_status = kv.abort_write_batch();
    if (abort_status.is_error()) {
      LOG(INFO) << "Failed to abort large-account cache write batch: " << abort_status;
    }
    return status;
  }
  return kv.commit_write_batch();
}

void LargeAccountCache::set_before_write_test_hook(std::function<void()> hook) {
  std::lock_guard<std::mutex> lock(state_->before_write_test_hook_mutex);
  state_->before_write_test_hook = std::move(hook);
}

void LargeAccountCacheTestAccess::set_before_write_hook(LargeAccountCache& cache, std::function<void()> hook) {
  cache.set_before_write_test_hook(std::move(hook));
}

}  // namespace ton::validator
