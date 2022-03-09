#include "cache.h"
#include <stdexcept>

namespace ROCKSDB_NAMESPACE {
  cache::cache(): cache(DEFAULT_CACHE_SIZE) { }

  cache::cache(uint64_t c): capacity(c) {
    map = new robin_hood::unordered_map<string, cache_entry*>(capacity);
    policy = new lfu_policy(capacity);
  }

  cache::cache(cache_options &options) {
    capacity = options.numEntries;
    map = new robin_hood::unordered_map<string, cache_entry*>(capacity);

    switch (options.policy) {
      case lookaside_cache_policy::CACHE_POLICY_LFU:
      default:  // temporary
        {
          policy = new lfu_policy(capacity);
          break;
        }
    }
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
    cache_entry *res;

    try {
      res = this->map->at(key);
      policy->MarkAccess(key, res);
      RecordTick(stats_, LOOKASIDE_CACHE_HIT);
    } catch(const out_of_range& e) {
      if (markMiss) {
        RecordTick(stats_, LOOKASIDE_CACHE_MISS);
      }
      return nullptr;
    }

    return res;
  }

  void cache::Insert(string& key, string* value) {
    if (Lookup(key, false)) {
      // TODO should we call `MarkAccess()` here?
      return;
    }

    if (map->size() == capacity) {
      map->erase(policy->Evict());
      RecordTick(stats_, LOOKASIDE_CACHE_EVICTION);
    }

    cache_entry *newEntry = new cache_entry(value);
    (*map)[key] = newEntry;
    policy->MarkInsertion(key, newEntry);
  }

  void cache::Update(Slice& keySlice, string* updatedValue) {
    string key = keySlice.ToString();

    cache_entry *maybeEntry = Lookup(key, false);
    if (!maybeEntry) {
      // TODO should we call `MarkAccess()` here?
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

    delete maybeEntry;
  }
}
