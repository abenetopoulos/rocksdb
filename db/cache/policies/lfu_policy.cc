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

  lfu_policy::lfu_policy() {
    frequencies = nullptr;
    reusableNodes = nullptr;
  }

  void lfu_policy::MarkInsertion(string& key, cache_entry *cacheEntry) {
    lfu_key_node *keyNode = NewKeyNode(key);

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

  void lfu_policy::MarkAccess(string& key, cache_entry *cacheEntry) {
    lfu_key_node *keyNode = (lfu_key_node *) cacheEntry->extra;
    (void) key;
    // assert(keyNode->key == key);
    lfu_frequency_node* frequencyNode = keyNode->frequencyNode;

    lfu_frequency_node* newFrequencyNode = frequencyNode->next;
    if (!newFrequencyNode || newFrequencyNode->frequency != (frequencyNode->frequency + 1)) {
      newFrequencyNode = new lfu_frequency_node(frequencyNode->frequency + 1);

      newFrequencyNode->prev = frequencyNode;
      newFrequencyNode->next = frequencyNode->next;
      frequencyNode->next = newFrequencyNode;
    }

    frequencyNode->ExchangeKey(keyNode, newFrequencyNode);

#ifndef LA_CACHE_KEEP_EMPTY_FREQ_NODES
    if (!frequencyNode->keys) {
      DeleteFrequencyNode(frequencyNode);
    }
#endif
  }


  string lfu_policy::EvictKeyNode(lfu_key_node *nodeToEvict) {
    lfu_frequency_node *correspondingFrequencyNode = nodeToEvict->frequencyNode;
    string res = nodeToEvict->key;

    correspondingFrequencyNode->keys = nodeToEvict->next;
    if (nodeToEvict->next) {
      nodeToEvict->next->prev = nodeToEvict->prev;
    }
    if (nodeToEvict->prev) {
      nodeToEvict->prev->next = nodeToEvict->next;
    }

    ReclaimNode(nodeToEvict);

#ifndef LA_CACHE_KEEP_EMPTY_FREQ_NODES
    if (!correspondingFrequencyNode->keys) {
      DeleteFrequencyNode(correspondingFrequencyNode);
    }
#endif

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

    if (nodeToDelete->prev) {
      nodeToDelete->prev->next = nodeToDelete->next;
    }
    if (nodeToDelete->next) {
      nodeToDelete->next->prev = nodeToDelete->prev;
    }
    if (frequencies == nodeToDelete) {
      frequencies = nodeToDelete->next;
    }

    delete nodeToDelete;
  }

  void lfu_policy::ReclaimNode(lfu_key_node *keyNode) {
    keyNode->next = reusableNodes;
    keyNode->frequencyNode = nullptr;

    reusableNodes = keyNode;
  }

  lfu_key_node* lfu_policy::NewKeyNode(string& key) {
    if (!reusableNodes) {
      return new lfu_key_node(key);
    }

    lfu_key_node *res = reusableNodes;
    reusableNodes = res->next;

    res->key = key;

    return res;
  }

  const string lfu_policy::NO_FREQUENCY_INFO = "ELFUPOLICYNOFREQINFO";
}

