#include "cache.h"

namespace ROCKSDB_NAMESPACE {
  lfu_key_node::lfu_key_node(string k): key(k) {
    prev = nullptr;
    next = nullptr;

    frequencyNode = nullptr;
  }

  lfu_frequency_node::lfu_frequency_node(): lfu_frequency_node(1) { }

  lfu_frequency_node::lfu_frequency_node(uint64_t f): frequency(f) {
    prev = nullptr;
    next = nullptr;

    keys = nullptr;
  }

  void lfu_frequency_node::AddKey(lfu_key_node *keyNode) {
    keyNode->frequencyNode = this;

    keyNode->prev = nullptr;
    keyNode->next = keys;

    keys = keyNode;
  }

  void lfu_frequency_node::RemoveKey(lfu_key_node *keyNode) {
    assert(keyNode->frequencyNode == this);

    if (keyNode->prev) {
      keyNode->prev->next = keyNode->next;
    } else {
      // since the given key node is at the head of the key node list,
      // we need to update the frequency node's pointer to point to the
      // key node's successor.
      this->keys = keyNode->next;
    }

    if (keyNode->next) {
      keyNode->next->prev = keyNode->prev;
    }

    keyNode->frequencyNode = nullptr;
  }

  void lfu_frequency_node::ExchangeKey(lfu_key_node *keyNode, lfu_frequency_node *targetFrequencyNode) {
    this->RemoveKey(keyNode);
    targetFrequencyNode->AddKey(keyNode);
  }

  lfu_policy::lfu_policy(uint64_t capacity) {
    map = new robin_hood::unordered_map<string, lfu_key_node*>(capacity);
    frequencies = nullptr;
  }

  void lfu_policy::MarkInsertion(string& key) {
    lfu_key_node *keyNode = new lfu_key_node(key);
    (*map)[key] = keyNode;

    lfu_frequency_node* frequencyNode = frequencies;

    if (!frequencyNode || frequencyNode->frequency != 1) {
      lfu_frequency_node* newFrequencyNode = new lfu_frequency_node(1);

      newFrequencyNode->next = frequencyNode;
      if (frequencyNode) {
        frequencyNode->prev = newFrequencyNode;
      }

      frequencyNode = newFrequencyNode;
      frequencies = frequencyNode;
    }

    frequencyNode->AddKey(keyNode);
  }

  void lfu_policy::MarkAccess(string& key) {
    lfu_key_node* keyNode = (*map)[key];
    lfu_frequency_node* frequencyNode = keyNode->frequencyNode;

    lfu_frequency_node* newFrequencyNode = frequencyNode->next;
    if (!newFrequencyNode || newFrequencyNode->frequency != (frequencyNode->frequency + 1)) {
      newFrequencyNode = new lfu_frequency_node(frequencyNode->frequency + 1);

      newFrequencyNode->prev = frequencyNode;
      newFrequencyNode->next = frequencyNode->next;
      frequencyNode->next = newFrequencyNode;
    }

    frequencyNode->ExchangeKey(keyNode, newFrequencyNode);

    if (!frequencyNode->keys) {
        DeleteFrequencyNode(frequencyNode);
    }
  }

  string lfu_policy::Evict() {
    if (!frequencies || !frequencies->keys) {
      return NO_FREQUENCY_INFO;
    }

    lfu_key_node* nodeToEvict = frequencies->keys;

    string res = nodeToEvict->key;
    frequencies->keys = nodeToEvict->next;
    if (nodeToEvict->next) {
      nodeToEvict->next->prev = nodeToEvict->prev;
    }
    if (nodeToEvict->prev) {
      nodeToEvict->prev->next = nodeToEvict->next;
    }

    map->erase(nodeToEvict->key);
    delete nodeToEvict;

    if (!frequencies->keys) {
      DeleteFrequencyNode(frequencies);
    }

    return res;
  }

  void lfu_policy::DeleteFrequencyNode(lfu_frequency_node* frequencyNode) {
    lfu_frequency_node *nodeToDelete = frequencyNode;

    frequencies = nodeToDelete->next;
    if (nodeToDelete->prev) {
      nodeToDelete->prev->next = frequencies;
    }
    if (nodeToDelete->next) {
      nodeToDelete->next->prev = nodeToDelete->prev;
    }

    delete nodeToDelete;
  }

  const string lfu_policy::NO_FREQUENCY_INFO = "ELFUPOLICYNOFREQINFO";

  cache::cache(): cache(DEFAULT_CACHE_SIZE) { }

  cache::cache(uint64_t c): capacity(c) {
    map = new robin_hood::unordered_map<string, string*>(capacity);
    policy = new lfu_policy(capacity);
  }

  cache::cache(cache_options &options) {
    capacity = options.numEntries;
    map = new robin_hood::unordered_map<string, string*>(capacity);

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
    return Lookup(key);
  }

  string* cache::Lookup(Slice &keySlice) {
    string key = keySlice.ToString();
    return Lookup(key);
  }

  void cache::Insert(LookupKey &lkey, string* value) {
    string key = lkey.user_key().ToString();
    Insert(key, value);
  }

  void cache::Insert(Slice &keySlice, string* value) {
    string key = keySlice.ToString();
    Insert(key, value);
  }

  string* cache::Lookup(string& key, bool markMiss) {
    string* res;

    try {
      res = this->map->at(key);
      policy->MarkAccess(key);
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

    (*map)[key] = value;
    policy->MarkInsertion(key);
  }

  void cache::Update(Slice& keySlice, string* updatedValue) {
    string key = keySlice.ToString();

    if (Lookup(key, false)) {
      // TODO should we call `MarkAccess()` here?
      return;
    }

    (*this->map)[key] = updatedValue;
    policy->MarkAccess(key);
  }

  const string cache::NOT_FOUND = "ECACHENOTFOUND";
}
