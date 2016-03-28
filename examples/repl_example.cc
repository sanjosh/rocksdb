// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <iostream>
#include <string>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_repl_example";

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleNone;
  options.repl_addr = "127.0.0.1";
  options.repl_port = 8192;
  options.info_log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  {
    std::string value = "uniqueval";
    s = db->Put(WriteOptions(), "key1", value);
    assert(s.ok());
  }

  // atomically apply a set of updates
  {
    std::string value = "uniqueval";
    WriteBatch batch;
    batch.Put("key2", value);
    batch.Put("key3", value);
    s = db->Write(WriteOptions(), &batch);
  }

  // Put key-value
  for (int i = 0; i < 100; i++) 
  {
    std::string key = "key_" + std::to_string(i);
    std::string value = "value_" + std::to_string(10 + i);
    s = db->Put(WriteOptions(), key, value);
    assert(s.ok());
    usleep(100); // delayed write
  }

  for (int i = 0; i < 100; i++) 
  {
    std::string returnValue;
    std::string key = "key_" + std::to_string(i);
    s = db->Get(ReadOptions(), key, &returnValue);
    std::cout << "Obtained key=" << key 
      << ":status=" << s.ToString() 
      << ":value=" << returnValue
      << std::endl;
  }

  {
    WriteBatch batch;
    for (int i = 0; i < 100; i++) {
      std::string key = "key_" + std::to_string(i);
      batch.Delete(key);
    }
    s = db->Write(WriteOptions(), &batch);
    assert(s.ok());
  }
  
  usleep(10000);

  for (int i = 0; i < 100; i++) 
  {
    std::string returnValue;
    std::string key = "key_" + std::to_string(i);
    s = db->Get(ReadOptions(), key, &returnValue);
    std::cout << "Obtained key=" << key 
      << ":status=" << s.ToString() 
      << ":value=" << returnValue
      << std::endl;
  }

  usleep(10000);

  delete db;

  return 0;
}
