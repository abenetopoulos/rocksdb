#pragma once

#include "db/lookup_key.h"
#include "db/cache/cache_entry.h"
#include "robin_hood.h"
#include "policies/lfu_policy.h"
#include "monitoring/statistics.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"


using namespace std;

// default capacity for cache in terms of number of entries
#define DEFAULT_CACHE_SIZE 1024

namespace ROCKSDB_NAMESPACE {
  struct cache_entry;
  // TODOs (in priority order)
  // - Support more policies: for now, we can either switch the type of the `policy` member to be
  //    `eviction_policy*`, _or_ we can have one member for each policy
  //    type, and instantiate/use the appropriate one based on the policy
  //    we get passed at init. Performance-wise, the second solution should
  //    be better (avoiding vtable lookups etc.), but it is uglier.
  // - consider turning this into a template to parameterize the value type
  struct cache {
    static const string NOT_FOUND;
    robin_hood::unordered_map<string, cache_entry*> *map;
    uint64_t capacity;

    // NOTE this is here simply because I am lazy.
    Statistics* stats_;

    lfu_policy *policy;

    cache();
    cache(uint64_t c);
    cache(cache_options &options);

    // convenience functions
    string* Lookup(LookupKey &lkey);
    string* Lookup(Slice &keySlice);
    void Insert(LookupKey &lkey, string* value);
    void Insert(Slice &keySlice, string* value);
    // void Update(Slice& keySlice, Slice& updatedValueSlice);

    cache_entry* Lookup(string& key, bool markMiss = true);
    void Insert(string& key, string* value);
    void Update(Slice& keySlice, string* updatedValue);
    void Remove(Slice& keySlice);
  };
}
