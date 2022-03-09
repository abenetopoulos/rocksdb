#include "lfu_policy.h"

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
    if (keys) {
      keys->prev = keyNode;
    }

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

  void lfu_policy::MarkInsertion(string& key, cache_entry *cacheEntry) {
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

    cacheEntry->extra = (void *) keyNode;
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


  string lfu_policy::EvictKeyNode(lfu_key_node *nodeToEvict) {
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

  string lfu_policy::Evict() {
    if (!frequencies || !frequencies->keys) {
      return NO_FREQUENCY_INFO;
    }

    lfu_key_node* nodeToEvict = frequencies->keys;
    return EvictKeyNode(nodeToEvict);
  }

  string lfu_policy::Evict(cache_entry *cacheEntry) {
    if (!cacheEntry->extra) {
      return "";  // TODO appropriate return value
    }

    lfu_key_node* nodeToEvict = (lfu_key_node *) cacheEntry->extra;
    return EvictKeyNode(nodeToEvict);
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
}

