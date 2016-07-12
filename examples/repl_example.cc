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

static constexpr size_t NumKeys = 5;

static void waitForUser()
{
  std::cout << "enter char to continue" << std::endl;
  getchar();
}

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

  rocksdb::WriteOptions writeOpt;
  writeOpt.sync = true;
  // Put key-value
  for (int i = 0; i < NumKeys; i++) 
  {
    std::string key = "key_" + std::to_string(i);
    std::string value(4096, 'a' + i);
    s = db->Put(writeOpt, key, value);
    assert(s.ok());
  }

  waitForUser();

  for (int i = 0; i < NumKeys; i++) 
  {
    std::string returnValue;
    std::string key = "key_" + std::to_string(i);
    s = db->Get(ReadOptions(), key, &returnValue);
    std::cout << "Obtained key=" << key 
      << ":status=" << s.ToString() 
      << ":value=" << returnValue.substr(0, 10)
      << std::endl;
  }

  waitForUser();

  auto snap = db->GetSnapshot();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = snap;

  auto iter = db->NewIterator(read_options);
  std::string seekKey = "key_3";
  for (iter->Seek(seekKey); iter->Valid(); iter->Next())
  {
    std::cout 
      << "Cursor key=" << std::hex << iter->key().ToString() << std::dec
      << " value=" << iter->value().ToString().substr(0, 10)
      << std::endl;
  }
  delete iter;
  db->ReleaseSnapshot(snap);

  waitForUser();

  {
    WriteBatch batch;
    for (int i = 0; i < NumKeys; i++) {
      std::string key = "key_" + std::to_string(i);
      batch.Delete(key);
    }
    s = db->Write(writeOpt, &batch);
    assert(s.ok());
  }
  
  waitForUser();

  for (int i = 0; i < NumKeys; i++) 
  {
    std::string returnValue;
    std::string key = "key_" + std::to_string(i);
    s = db->Get(ReadOptions(), key, &returnValue);
    std::cout << "Obtained key=" << key 
      << ":status=" << s.ToString() 
      << ":value_empty=" << returnValue.empty()
      << std::endl;
  }

  waitForUser();

  delete db;

  return 0;
}
