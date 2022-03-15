#pragma once

#include "eviction_policy.h"
#include "monitoring/statistics.h"
#include "robin_hood.h"

using namespace std;

namespace ROCKSDB_NAMESPACE {
  struct lfu_frequency_node;

  struct lfu_key_node {
    string key;

    lfu_key_node *prev;
    lfu_key_node *next;
    lfu_frequency_node *frequencyNode;

    lfu_key_node(string);
  };

  struct lfu_frequency_node {
    uint64_t frequency;

    lfu_frequency_node *prev;
    lfu_frequency_node *next;

    lfu_key_node *keys;

    lfu_frequency_node();
    lfu_frequency_node(uint64_t f);

    void AddKey(lfu_key_node *keyNode);
    void RemoveKey(lfu_key_node *keyNode);
    void ExchangeKey(lfu_key_node *keyNode, lfu_frequency_node *targetFrequencyNode);
  };

  // An implementation of a constant-time LFU cache.
  // http://dhruvbird.com/lfu.pdf
  struct lfu_policy: eviction_policy {
    // TODO
    //  - sanitize memory accesses to avoid null-ptr derefs
    static const string NO_FREQUENCY_INFO;

    lfu_frequency_node *frequencies;
    lfu_key_node *reusableNodes;

    lfu_policy();

    void MarkInsertion(string& key, cache_entry *cacheEntry);
    void MarkAccess(string& key, cache_entry *cacheEntry);

    string Evict();
    string Evict(cache_entry *cacheEntry);
    string EvictKeyNode(lfu_key_node *keyNode);

    void DeleteFrequencyNode(lfu_frequency_node* frequencyNode);

    void ReclaimNode(lfu_key_node *keyNode);
    lfu_key_node* NewKeyNode(string& key);
  };
}
