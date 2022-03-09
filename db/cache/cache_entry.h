#pragma once

#include <string>  // NOTE to silence the silly implicit-namespace errors

#include "rocksdb/rocksdb_namespace.h"

using namespace std;

namespace ROCKSDB_NAMESPACE {
  struct cache_entry {
    string *value;
    void *extra;

    cache_entry(string *v): value(v) { }
  };
}
