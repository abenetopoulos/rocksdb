#pragma once

#include <stdexcept>

#include "eviction_policy.h"

using namespace std;

namespace ROCKSDB_NAMESPACE {
  struct lru_key_node {
    string key;

    lru_key_node *prev;
    lru_key_node *next;

    lru_key_node(string key);
  };

  // An LRU policy implemented as a dequeue.
  // We are _kinda_ cheating in that we maintain a reference to the appropriate "key node"
  // in the main cache, so deletions and moves are cheap (no need for lookups).
  struct lru_policy: eviction_policy {
    lru_key_node *keysStart;
    lru_key_node *keysEnd;

    lru_key_node *reusableNodes;

    lru_policy();

    void InsertKeyNode(lru_key_node *keyNode);
    void RemoveKeyNode(lru_key_node *keyNode);

    void MarkInsertion(string& key, cache_entry *cacheEntry);
    void MarkAccess(string& key, cache_entry *cacheEntry);
    string Evict();
    string Evict(cache_entry *cacheEntry);
    string EvictKeyNode(lru_key_node *keyNode);

    void ReclaimNode(lru_key_node *keyNode);
    lru_key_node* NewKeyNode(string& key);
  };
}
