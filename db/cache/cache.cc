#include "cache.h"
#include <stdexcept>

namespace ROCKSDB_NAMESPACE {
  cache::cache(): cache(DEFAULT_CACHE_SIZE) { }

  cache::cache(uint64_t c): capacity(c) {
    map = new robin_hood::unordered_map<string, cache_entry*>(capacity);
    policy = new LAC_POLICY_C(capacity);
  }

  cache::cache(cache_options &options) {
    capacity = options.numEntries;
    map = new robin_hood::unordered_map<string, cache_entry*>(capacity);

    policy = new LAC_POLICY_C(capacity);
  }

  string* cache::Lookup(LookupKey &lkey) {
    string key = lkey.user_key().ToString();
    cache_entry *res = Lookup(key);

    return res ? res->value : nullptr;
  }

  string* cache::Lookup(Slice &keySlice) {
    string key = keySlice.ToString();
    cache_entry *res = Lookup(key);

    return res ? res->value : nullptr;
  }

  void cache::Insert(LookupKey &lkey, string* value) {
    string key = lkey.user_key().ToString();
    Insert(key, value);
  }

  void cache::Insert(Slice &keySlice, string* value) {
    string key = keySlice.ToString();
    Insert(key, value);
  }

  cache_entry* cache::Lookup(string& key, bool markMiss) {
#ifndef LA_DISALLOW_NULL_ENTRIES
    cache_entry *res = (*map)[key];

    if (res != nullptr) {
      RecordTick(stats_, LOOKASIDE_CACHE_HIT);
      return res;
    }
#else
    auto res = map->find(key);
    if (res != map->end()) {
      RecordTick(stats_, LOOKASIDE_CACHE_HIT);
      return res->second;
    }
#endif

    if (markMiss) {
      RecordTick(stats_, LOOKASIDE_CACHE_MISS);
    }

    return nullptr;
  }

  void cache::Insert(string& key, string* value) {
    if (Lookup(key, false)) {
      // TODO should we call `MarkAccess()` here?
      return;
    }

    while (numResidentElements >= capacity) {
      map->erase(policy->Evict());
      numResidentElements--;
      RecordTick(stats_, LOOKASIDE_CACHE_EVICTION);
    }

    cache_entry *newEntry = new cache_entry(value);
    (*map)[key] = newEntry;
    policy->MarkInsertion(key, newEntry);
    numResidentElements++;
  }

  void cache::Update(Slice& keySlice, string* updatedValue) {
    string key = keySlice.ToString();

    cache_entry *maybeEntry = Lookup(key, false);
    if (!maybeEntry) {
      Insert(key, updatedValue);
      return;
    }

    maybeEntry->value = updatedValue;
    policy->MarkAccess(key, maybeEntry);
  }

  const string cache::NOT_FOUND = "ECACHENOTFOUND";

  void cache::Remove(Slice& keySlice) {
    string key = keySlice.ToString();
    cache_entry *maybeEntry = Lookup(key, false);
    if (!maybeEntry) {
      return;
    }

    if (!policy->Evict(maybeEntry).size()) {
      // TODO
      throw std::runtime_error("Failed to evict key node for key");
    }

    numResidentElements--;
    delete maybeEntry;
  }
}
