// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <cstdio>
#include <string>
#include <vector>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"

using namespace rocksdb;

std::string kDBPath = "/tmp/rocksdb_repl_column_families";

int main() {
  // open DB
  Options options;
  options.create_if_missing = true;
  options.repl_addr = "127.0.0.1";
  options.repl_port = 8192;
  options.info_log_level = rocksdb::InfoLogLevel::DEBUG_LEVEL;

  DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // create column family
  std::vector<ColumnFamilyHandle*> handles;
  for (int i = 0; i < 40; i++) {
    ColumnFamilyHandle* cf;
    std::string name = "new_cf_" + std::to_string(i);
    s = db->CreateColumnFamily(ColumnFamilyOptions(), name, &cf);
    assert(s.ok());
    handles.push_back(cf);
 }

  // atomic write
  for (int i = 0; i < 40; i++) {
    WriteBatch batch;
    batch.Put(handles[i], Slice("key2"), Slice("value2"));
    batch.Put(handles[i], Slice("key3"), Slice("value3"));
    batch.Delete(handles[i], Slice("key"));
    s = db->Write(WriteOptions(), &batch);
    assert(s.ok());
    usleep(100);
  }

  // drop column family
  for (auto elem : handles) {
    s = db->DropColumnFamily(elem);
    assert(s.ok());
  }

  usleep(100000);// wait for replication 
  delete db;

  return 0;
}
