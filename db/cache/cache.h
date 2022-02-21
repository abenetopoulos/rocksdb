#pragma once

#include "robin_hood.h"
#include "rocksdb/db.h"


using namespace std;

// default capacity for cache in terms of number of entries
#define DEFAULT_CACHE_SIZE 1024

namespace ROCKSDB_NAMESPACE {
    // TODO consider making this a template
    struct eviction_policy {
        virtual void MarkInsertion(Slice& key);
        virtual void MarkAccess(Slice& key);
        virtual string Evict();

        virtual ~eviction_policy() { }
    };

    struct lfu_frequency_node;

    struct lfu_key_node {
        string key;

        lfu_key_node *prev;
        lfu_key_node *next;
        lfu_frequency_node *frequencyNode;

        lfu_key_node(string k): key(k) {
            prev = nullptr;
            next = nullptr;
            frequencyNode = nullptr;
        }
    };

    struct lfu_frequency_node {
        uint64_t frequency;

        lfu_frequency_node *prev;
        lfu_frequency_node *next;

        lfu_key_node *keys;

        lfu_frequency_node(): lfu_frequency_node(1) { }

        lfu_frequency_node(uint64_t f): frequency(f) {
            prev = nullptr;
            next = nullptr;

            keys = nullptr;
        }
    };

    // An implementation of a constant-time LFU cache.
    // http://dhruvbird.com/lfu.pdf
    struct lfu_policy: eviction_policy {
        // TODO
        //  - sanitize memory accesses to avoid null-ptr derefs
        //  - change from circular lists to linear ones

        robin_hood::unordered_map<string, lfu_key_node*> *map;
        lfu_frequency_node *frequencies;

        lfu_policy(uint64_t capacity) {
            map = new robin_hood::unordered_map<string, lfu_key_node*>(capacity);
            frequencies = nullptr;
        }

        void MarkInsertion(Slice& key) {
            lfu_key_node *keyNode = new lfu_key_node(key.data());
            (*map)[key.data()] = keyNode;

            lfu_frequency_node* frequencyNode = frequencies;

            if (!frequencyNode || frequencyNode->frequency != 1) {
                lfu_frequency_node* newFrequencyNode = new lfu_frequency_node(1);

                newFrequencyNode->next = frequencyNode;
                if (frequencyNode) {
                    frequencyNode->prev = newFrequencyNode;
                }

                frequencyNode = newFrequencyNode;
            }

            keyNode->next = frequencyNode->keys;
            if (frequencyNode->keys) {
                keyNode->prev = frequencyNode->keys->prev;
            }
            frequencyNode->keys = keyNode;
            keyNode->frequencyNode = frequencyNode;
        }

        void MarkAccess(Slice& key) {
            lfu_key_node* keyNode = (*map)[key.data()];
            lfu_frequency_node* frequencyNode = keyNode->frequencyNode;

            lfu_frequency_node* newFrequencyNode = frequencyNode->next;
            if (!newFrequencyNode || newFrequencyNode->frequency != (frequencyNode->frequency + 1)) {
                newFrequencyNode = new lfu_frequency_node(frequencyNode->frequency + 1);
                newFrequencyNode->prev = frequencyNode;
                newFrequencyNode->next = frequencyNode->next;
                frequencyNode->next = newFrequencyNode;
            }

            frequencyNode->keys = keyNode->next;
            keyNode->frequencyNode = newFrequencyNode;
            keyNode->next->prev = keyNode->prev;
            keyNode->prev->next = keyNode->next;

            keyNode->prev = nullptr;
            keyNode->next = newFrequencyNode->keys;

            if (!frequencyNode->keys) {
                frequencyNode->next->prev = frequencyNode->prev;
                frequencyNode->prev->next = frequencyNode->next;
                delete frequencyNode;
            }
        }

        string Evict() {
            string res = "";

            if (!frequencies || !frequencies->keys) {
                // FIXME
                return res;
            }

            lfu_key_node* nodeToEvict = frequencies->keys;

            res = nodeToEvict->key;
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

        void DeleteFrequencyNode(lfu_frequency_node* frequencyNode) {
            lfu_frequency_node *nodeToDelete = frequencyNode;

            frequencies = nodeToDelete->next;
            if (nodeToDelete->prev) {
                nodeToDelete->prev->next = nodeToDelete->next;
            }
            if (nodeToDelete->next) {
                nodeToDelete->next->prev = nodeToDelete->prev;
            }

            delete nodeToDelete;
        }
    };

    // TODO consider turning this into a template to parameterize the value type
    struct cache {
        robin_hood::unordered_map<string, string> *map;
        uint64_t capacity;

        eviction_policy *policy;

        cache(): cache(DEFAULT_CACHE_SIZE) { }

        cache(uint64_t c): capacity(c) {
            map = new robin_hood::unordered_map<string, string>(capacity);
            policy = new lfu_policy(capacity);
        }

        string Lookup(Slice& key) {
            string res;

            try {
                res = this->map->at(key.data());
                policy->MarkAccess(key);
            } catch(const std::out_of_range& e) {
                // FIXME
                res = "";
            }

            return res;
        }

        void Insert(Slice& key, Slice& value) {
            if (Lookup(key) != "") {
                // TODO should we call `MarkAccess()` here?
                return;
            }

            if (map->size() == capacity) {
                string evictedKey = policy->Evict();
            }

            (*this->map)[key.data()] = value.data();
            policy->MarkInsertion(key);
        }
    };
}
