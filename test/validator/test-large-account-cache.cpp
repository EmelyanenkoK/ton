#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "td/actor/actor.h"
#include "td/utils/port/path.h"
#include "td/utils/tests.h"
#include "validator/large-account-cache.hpp"
#include "validator/storage-stat-cache.hpp"
#include "vm/boc.h"
#include "vm/cells.h"

namespace ton::validator {
namespace {

class TempDir {
 public:
  explicit TempDir(td::Slice prefix) {
    path_ = td::mkdtemp(td::CSlice(), prefix).move_as_ok();
  }

  ~TempDir() {
    td::rmrf(path_).ignore();
  }

  const std::string& path() const {
    return path_;
  }

 private:
  std::string path_;
};

td::Bits256 cell_hash(const td::Ref<vm::Cell>& cell) {
  return td::Bits256{cell->get_hash().bits()};
}

struct CacheEntryData {
  td::Ref<vm::Cell> dict_root;
  std::vector<td::Ref<vm::Cell>> roots;

  td::Bits256 hash() const {
    return cell_hash(dict_root);
  }
};

td::Ref<vm::Cell> make_cell(const std::string& data, std::vector<td::Ref<vm::Cell>> refs = {}) {
  vm::CellBuilder builder;
  builder.store_bytes(data);
  for (auto& ref : refs) {
    builder.store_ref(ref);
  }
  return builder.finalize();
}

CacheEntryData make_entry(char tag) {
  auto tag_str = std::string(1, tag);
  auto shared_left = make_cell("shared-left-" + tag_str);
  auto shared_right = make_cell("shared-right-" + tag_str);
  CacheEntryData entry;
  entry.dict_root = make_cell("dict-root-" + tag_str, {shared_left, shared_right});
  entry.roots.push_back(make_cell("root-a-" + tag_str, {shared_left}));
  entry.roots.push_back(make_cell("root-b-" + tag_str, {shared_right}));
  return entry;
}

StdSmcAddress make_address(char hex_digit) {
  StdSmcAddress addr;
  CHECK(addr.from_hex(std::string(64, hex_digit)) == 256);
  return addr;
}

LargeAccountCacheUpdate make_update(StdSmcAddress addr, const CacheEntryData& entry,
                                    td::Timestamp enqueued_at = td::Timestamp::now()) {
  LargeAccountCacheUpdate update;
  update.workchain = basechainId;
  update.addr = addr;
  update.storage_dict_hash = entry.hash();
  update.dict_root = entry.dict_root;
  update.roots = entry.roots;
  update.enqueued_at = enqueued_at;
  return update;
}

LargeAccountCacheUpdate make_remove_update(StdSmcAddress addr, td::Timestamp enqueued_at = td::Timestamp::now()) {
  LargeAccountCacheUpdate update;
  update.workchain = basechainId;
  update.addr = addr;
  update.enqueued_at = enqueued_at;
  return update;
}

td::uint64 serialized_entry_size(const CacheEntryData& entry) {
  auto dict_root_boc = vm::std_boc_serialize(entry.dict_root).move_as_ok();
  auto roots_boc = vm::std_boc_serialize_multi(entry.roots).move_as_ok();
  return 1 + 8 + 8 + static_cast<td::uint64>(dict_root_boc.size()) + static_cast<td::uint64>(roots_boc.size());
}

LargeAccountCacheAccess get_large_account_cache_access(LargeAccountCache& cache) {
  LargeAccountCacheAccess access;
  bool ready = false;
  cache.get_access(td::PromiseCreator::lambda([&](td::Result<LargeAccountCacheAccess> result) {
    access = result.move_as_ok();
    ready = true;
  }));
  CHECK(ready);
  return access;
}

std::function<td::Ref<vm::Cell>(const td::Bits256&)> get_storage_stat_lookup(StorageStatCache& cache) {
  std::function<td::Ref<vm::Cell>(const td::Bits256&)> lookup;
  bool ready = false;
  cache.get_cache(td::PromiseCreator::lambda(
      [&](td::Result<std::function<td::Ref<vm::Cell>(const td::Bits256&)>> result) {
        lookup = result.move_as_ok();
        ready = true;
      }));
  CHECK(ready);
  return lookup;
}

void assert_lookup_matches(const LargeAccountCacheAccess& access, const CacheEntryData& entry) {
  auto value = access.lookup(entry.hash());
  ASSERT_TRUE(static_cast<bool>(value));
  ASSERT_EQ(cell_hash(value.value().dict_root), entry.hash());
  ASSERT_EQ(value.value().roots.size(), entry.roots.size());
  for (std::size_t i = 0; i < entry.roots.size(); ++i) {
    ASSERT_EQ(cell_hash(value.value().roots[i]), cell_hash(entry.roots[i]));
  }
}

}  // namespace

TEST(LargeAccountCache, PersistentRoundTripAcrossRestart) {
  TempDir temp_dir("large-account-cache");
  auto db_path = temp_dir.path() + "/db";
  auto entry = make_entry('1');
  auto address = make_address('1');

  {
    LargeAccountCache cache(db_path, 1 << 20, 16384);
    cache.start_up();
    cache.update({make_update(address, entry)});
  }

  {
    LargeAccountCache cache(db_path, 1 << 20, 16384);
    cache.start_up();
    auto access = get_large_account_cache_access(cache);
    ASSERT_TRUE(access.enabled());
    ASSERT_EQ(access.min_account_cells, 16384u);
    assert_lookup_matches(access, entry);
  }
}

TEST(LargeAccountCache, ReplacementFreesOldEntryBeforeCapCheck) {
  TempDir temp_dir("large-account-cache");
  auto db_path = temp_dir.path() + "/db";
  auto first_entry = make_entry('1');
  auto second_entry = make_entry('2');
  auto cap_bytes = std::max(serialized_entry_size(first_entry), serialized_entry_size(second_entry));

  LargeAccountCache cache(db_path, cap_bytes, 16384);
  cache.start_up();
  auto access = get_large_account_cache_access(cache);
  auto address = make_address('1');

  cache.update({make_update(address, first_entry)});
  assert_lookup_matches(access, first_entry);

  cache.update({make_update(address, second_entry)});
  ASSERT_TRUE(!access.lookup(first_entry.hash()));
  assert_lookup_matches(access, second_entry);
}

TEST(LargeAccountCache, SharedHashStaysUntilLastOwnerLeaves) {
  TempDir temp_dir("large-account-cache");
  auto db_path = temp_dir.path() + "/db";
  auto shared_entry = make_entry('3');
  auto replacement_entry = make_entry('4');

  LargeAccountCache cache(db_path, 1 << 20, 16384);
  cache.start_up();
  auto access = get_large_account_cache_access(cache);

  auto first_account = make_address('3');
  auto second_account = make_address('4');

  cache.update({make_update(first_account, shared_entry), make_update(second_account, shared_entry)});
  assert_lookup_matches(access, shared_entry);

  cache.update({make_update(first_account, replacement_entry)});
  assert_lookup_matches(access, shared_entry);
  assert_lookup_matches(access, replacement_entry);

  cache.update({make_remove_update(second_account)});
  ASSERT_TRUE(!access.lookup(shared_entry.hash()));
  assert_lookup_matches(access, replacement_entry);
}

TEST(LargeAccountCache, StaleUpdatesAreDropped) {
  TempDir temp_dir("large-account-cache");
  auto db_path = temp_dir.path() + "/db";
  auto entry = make_entry('5');
  auto address = make_address('5');

  LargeAccountCache cache(db_path, 1 << 20, 16384);
  cache.start_up();
  auto access = get_large_account_cache_access(cache);

  cache.update({make_update(address, entry, td::Timestamp::now() - 10.0)});
  ASSERT_TRUE(!access.lookup(entry.hash()));
}

TEST(LargeAccountCache, LookupSeesInFlightWrite) {
  TempDir temp_dir("large-account-cache");
  auto db_path = temp_dir.path() + "/db";
  auto entry = make_entry('6');
  auto address = make_address('6');

  LargeAccountCache cache(db_path, 1 << 20, 16384);
  cache.start_up();
  auto access = get_large_account_cache_access(cache);

  std::mutex mutex;
  std::condition_variable cv;
  bool write_is_paused = false;
  bool continue_write = false;
  LargeAccountCacheTestAccess::set_before_write_hook(cache, [&] {
    std::unique_lock<std::mutex> lock(mutex);
    write_is_paused = true;
    cv.notify_all();
    cv.wait(lock, [&] { return continue_write; });
  });

  std::thread writer([&] {
    cache.update({make_update(address, entry)});
  });

  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return write_is_paused; });
  }

  assert_lookup_matches(access, entry);

  {
    std::lock_guard<std::mutex> lock(mutex);
    continue_write = true;
  }
  cv.notify_all();
  writer.join();

  assert_lookup_matches(access, entry);
}

TEST(StorageStatCache, LiveLookupSeesLaterUpdates) {
  StorageStatCache cache;
  auto lookup = get_storage_stat_lookup(cache);

  auto small_cell = make_cell("small-storage-stat");
  auto small_hash = cell_hash(small_cell);
  cache.update({{small_cell, StorageStatCache::MIN_ACCOUNT_CELLS - 1}});
  ASSERT_TRUE(lookup(small_hash).is_null());

  auto large_cell = make_cell("large-storage-stat", {make_cell("storage-child")});
  auto large_hash = cell_hash(large_cell);
  ASSERT_TRUE(lookup(large_hash).is_null());

  cache.update({{large_cell, StorageStatCache::MIN_ACCOUNT_CELLS}});
  auto cached = lookup(large_hash);
  ASSERT_TRUE(cached.not_null());
  ASSERT_EQ(cell_hash(cached), large_hash);
}

}  // namespace ton::validator
