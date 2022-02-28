#pragma once

#include "db/lookup_key.h"
#include "robin_hood.h"
#include "monitoring/statistics.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"


using namespace std;

// default capacity for cache in terms of number of entries
#define DEFAULT_CACHE_SIZE 1024

namespace ROCKSDB_NAMESPACE {
  // TODO consider making this a template
  struct eviction_policy {
    virtual void MarkInsertion(string& key);
    virtual void MarkAccess(string& key);
    virtual string Evict();

    virtual ~eviction_policy() { }
  };

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

    ~lfu_frequency_node();
  };

  // An implementation of a constant-time LFU cache.
  // http://dhruvbird.com/lfu.pdf
  struct lfu_policy: eviction_policy {
    // TODO
    //  - sanitize memory accesses to avoid null-ptr derefs
    static const string NO_FREQUENCY_INFO;

    robin_hood::unordered_map<string, lfu_key_node*> *map;
    lfu_frequency_node *frequencies;

    lfu_policy(uint64_t capacity);

    void MarkInsertion(string& key);
    void MarkAccess(string& key);
    string Evict();
    void DeleteFrequencyNode(lfu_frequency_node* frequencyNode);
  };

  // TODO consider turning this into a template to parameterize the value type
  struct cache {
    static const string NOT_FOUND;
    // robin_hood::unordered_map<string, string> *map;
    robin_hood::unordered_map<string, string*> *map;
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

    string* Lookup(string& key);
    void Insert(string& key, string* value);
    void Update(Slice& keySlice, string* updatedValue);
  };
}
