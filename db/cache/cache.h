#pragma once

#include "robin_hood.h"
#include "rocksdb/db.h"


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

        void AddKey(lfu_key_node *keyNode) {
            keyNode->frequencyNode = this;

            keyNode->prev = nullptr;
            keyNode->next = keys;

            keys = keyNode;
        }

        void RemoveKey(lfu_key_node *keyNode) {
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

        void ExchangeKey(lfu_key_node *keyNode, lfu_frequency_node *targetFrequencyNode) {
            this->RemoveKey(keyNode);
            targetFrequencyNode->AddKey(keyNode);
        }

        ~lfu_frequency_node() {
            if (!this->prev && !this->next) {
                return;
            }

            if (this->prev) {
                this->prev->next = this->next;
            }

            if (this->next) {
                this->next->prev = this->prev;
            }
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

        void MarkInsertion(string& key) {
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
            }

            frequencyNode->AddKey(keyNode);
        }

        void MarkAccess(string& key) {
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
                delete frequencyNode;
            }
        }

        string Evict() {
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
        static const string NOT_FOUND;
        robin_hood::unordered_map<string, string> *map;
        uint64_t capacity;

        eviction_policy *policy;

        cache(): cache(DEFAULT_CACHE_SIZE) { }

        cache(uint64_t c): capacity(c) {
            map = new robin_hood::unordered_map<string, string>(capacity);
            policy = new lfu_policy(capacity);
        }

        string Lookup(Slice& keySlice) {
            string res;
            string key = keySlice.data();

            try {
                res = this->map->at(key);
                policy->MarkAccess(key);
            } catch(const std::out_of_range& e) {
                return NOT_FOUND;
            }

            return res;
        }

        void Insert(Slice& keySlice, Slice& valueSlice) {
            if (Lookup(keySlice) != NOT_FOUND) {
                // TODO should we call `MarkAccess()` here?
                return;
            }

            if (map->size() == capacity) {
                policy->Evict();
            }

            string key = keySlice.data();

            (*this->map)[key] = valueSlice.data();
            policy->MarkInsertion(key);
        }

        void Update(Slice& keySlice, Slice& updatedValueSlice) {
            if (Lookup(keySlice) != NOT_FOUND) {
                // TODO should we call `MarkAccess()` here?
                return;
            }

            string key = keySlice.data();

            (*this->map)[key] = updatedValueSlice.data();
            policy->MarkAccess(key);
        }
    };

    const string lfu_policy::NO_FREQUENCY_INFO = "ELFUPOLICYNOFREQINFO";
    const string cache::NOT_FOUND = "ECACHENOTFOUND";
}
