#include "lru_policy.h"

namespace ROCKSDB_NAMESPACE {
  lru_key_node::lru_key_node(string k): key(k) {
    prev = nullptr;
    next = nullptr;
  }

  lru_policy::lru_policy() {
    keysStart = nullptr;
    keysEnd = nullptr;
  }

  void lru_policy::InsertKeyNode(lru_key_node *keyNode) {
    keyNode->next = keysStart;
    if (keysStart) {
      keysStart->prev = keyNode;
    }
    keysStart = keyNode;

    if (keysEnd) {
      return;
    }

    keysEnd = keyNode;
  }

  void lru_policy::RemoveKeyNode(lru_key_node *keyNode) {
    if (keysStart == keyNode) {
      keysStart = keyNode->next;
    }

    if (keysEnd == keyNode) {
      keysEnd = keyNode->prev;
    }

    if (keyNode->prev) {
      keyNode->prev->next = keyNode->next;
    }

    if (keyNode->next) {
      keyNode->next->prev = keyNode->prev;
    }

    keyNode->next = nullptr;
    keyNode->prev = nullptr;
  }

  void lru_policy::MarkInsertion(string& key, cache_entry *cacheEntry) {
    lru_key_node *keyNode = new lru_key_node(key);

    InsertKeyNode(keyNode);

    cacheEntry->extra = (void *) keyNode;
  }

  void lru_policy::MarkAccess(string& key, cache_entry *cacheEntry) {
    lru_key_node *targetNode = (lru_key_node *) cacheEntry->extra;
    if (targetNode->key != key) {
      throw std::runtime_error("Failed to evict key node for key");
    }

    if (keysStart == targetNode) {
      // nothing to do, target is already the most-recently-used node
      return;
    }

    RemoveKeyNode(targetNode);
    InsertKeyNode(targetNode);
  }

  string lru_policy::Evict() {
    lru_key_node *nodeToEvict = keysEnd;
    return EvictKeyNode(nodeToEvict);
  }

  string lru_policy::Evict(cache_entry *cacheEntry) {
    lru_key_node *targetNode = (lru_key_node *) cacheEntry->extra;
    return EvictKeyNode(targetNode);
  }

  string lru_policy::EvictKeyNode(lru_key_node *keyNode) {
    string res = keyNode->key;

    RemoveKeyNode(keyNode);
    delete keyNode;

    return res;
  }
}
