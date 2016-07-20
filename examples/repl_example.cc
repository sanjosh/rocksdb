// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <iostream>
#include <string>
#include <unistd.h>
#include <thread>
#include <future>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_repl_example";

static size_t NumKeys = 100L;

static void waitForUser()
{
  std::cout << "enter char to continue" << std::endl;
  getchar();
}

DB* db;

static void Putter()
{
  rocksdb::WriteOptions writeOpt;
  // Put key-value
  for (decltype(NumKeys) i = 0; i < NumKeys; i++) 
  {
    std::string key = "key_" + std::to_string(i);
    std::string value(4096, 'a' + (i % 26));
    auto s = db->Put(writeOpt, key, value);
    assert(s.ok());
  }
}

static void Getter()
{
  decltype(NumKeys) count = 0;

  for (decltype(NumKeys) i = 0; i < NumKeys; i++) 
  {
    std::string returnValue;
    std::string key = "key_" + std::to_string(i);
    auto s = db->Get(ReadOptions(), key, &returnValue);
    if (s.ok()) {
      count ++;
    }
    /*
    std::cout << "Obtained key=" << key 
      << ":status=" << s.ToString() 
      << ":value=" << returnValue.substr(0, 10)
      << std::endl;
      */
  }
  std::cout << "num keys in get=" << count << std::endl;
}

static void Deleter()
{
  WriteBatch batch;
  rocksdb::WriteOptions writeOpt;

  for (decltype(NumKeys) i = 0; i < NumKeys; i++) {
    std::string key = "key_" + std::to_string(i);
    batch.Delete(key);
    if (i & ((i % 100) == 0)) {
      auto s = db->Write(writeOpt, &batch);
      std::cout << "finished deletes=" << i << std::endl;
      sleep(1);
    }
  }
  auto s = db->Write(writeOpt, &batch);
  assert(s.ok());
}

static void DoIter()
{
  //auto snap = db->GetSnapshot();
  rocksdb::ReadOptions read_options;
  //read_options.snapshot = snap;

  auto iter = db->NewIterator(read_options);
  //std::string seekKey = "key_3";
  std::string seekKey;
  decltype(NumKeys) count = 0;
  for (iter->Seek(seekKey); iter->Valid(); iter->Next())
  {
    std::cout 
      << "Cursor key=" << std::hex << iter->key().ToString() << std::dec
      << " value=" << iter->value().ToString().substr(0, 10)
      << std::endl;
    count ++;
  }
  std::cout << "num keys in iter=" << count << std::endl;
  delete iter;
  //db->ReleaseSnapshot(snap);
}

bool eof = false;

static void WriterThread()
{
  uint32_t count = 0;
  while (!eof)
  {
    std::cout << "start write round=" << count++ << std::endl;

    auto fut = std::async(std::launch::async,
      Putter);
    fut.wait();

    fut = std::async(std::launch::async,
      Deleter);
    fut.wait();
  }
  std::cout << "terminating writes at round=" << count << std::endl;
}

int main() {
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

  waitForUser();

  Putter();
  DoIter();

  /*
  auto write_fut = std::async(std::launch::async,
      WriterThread);

  std::vector<std::future<void>> futVec;

  for (int i = 0; i < 1000 ; i++)
  {
    std::cout << "start read round=" << i++ << std::endl;

    //auto fut1 = std::async(std::launch::async,
      //DoIter);
    //fut1.wait();

    auto fut2 = std::async(std::launch::async,
      Getter);
    fut2.wait();
  }

  eof = true;

  write_fut.wait();
  */

  delete db;

  return 0;
}
