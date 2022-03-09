#pragma once

#include "db/lookup_key.h"
#include "db/cache/cache_entry.h"
#include "robin_hood.h"
#include "policies/lfu_policy.h"
#include "policies/lru_policy.h"
#include "monitoring/statistics.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"


using namespace std;

// default capacity for cache in terms of number of entries
#define DEFAULT_CACHE_SIZE 1024

#ifdef LAC_POLICY_LRU
#define LAC_POLICY lru_policy
#define LAC_POLICY_C(c) lru_policy()
#else
#define LAC_POLICY lfu_policy
#define LAC_POLICY_C(c) lfu_policy((c))
#endif

namespace ROCKSDB_NAMESPACE {
  struct cache_entry;
  // TODOs (in priority order)
  // - consider turning this into a template to parameterize the value type
  struct cache {
    static const string NOT_FOUND;
    robin_hood::unordered_map<string, cache_entry*> *map;
    uint64_t capacity;

    // NOTE this is here simply because I am lazy.
    Statistics* stats_;

    LAC_POLICY *policy;

    cache();
    cache(uint64_t c);
    cache(cache_options &options);

    // convenience functions
    string* Lookup(LookupKey &lkey);
    string* Lookup(Slice &keySlice);
    void Insert(LookupKey &lkey, string* value);
    void Insert(Slice &keySlice, string* value);

    cache_entry* Lookup(string& key, bool markMiss = true);
    void Insert(string& key, string* value);
    void Update(Slice& keySlice, string* updatedValue);
    void Remove(Slice& keySlice);
  };
}
