#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "db/cache/cache_entry.h"

using namespace std;

namespace ROCKSDB_NAMESPACE {
  // TODO consider making this a template
  struct eviction_policy {
    virtual void MarkInsertion(string& key, cache_entry *cacheEntry) = 0;
    virtual void MarkAccess(string& key, cache_entry *cacheEntry) = 0;
    virtual string Evict() = 0;
    virtual string Evict(cache_entry *cacheEntry) = 0;

    virtual ~eviction_policy() { }
  };
}

