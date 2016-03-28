#pragma once

#include "rocksdb/types.h"
#include "rocksdb/status.h"
#include <string>
#include <atomic>

namespace rocksdb {

class Logger;
class DBImpl;
class MergeIteratorBuilder;
class EnvOptions;
class ReadOptions;
class ColumnFamilyHandle;
class Slice;

struct ReplThreadInfo {

  rocksdb::DBImpl* db = nullptr;
  std::shared_ptr<rocksdb::Logger> info_log = nullptr;

  std::atomic<bool> stop; // replace by cv
  std::atomic<bool> has_stopped;
  std::atomic<bool> started;

  int socket = -1; // used to send WAL to offloader
  int readSocket = -1; // used to query offloader
  int port = 0;
  std::string addr;

  Status Get(const ReadOptions& options, 
    ColumnFamilyHandle* column_family,
    const Slice& key, 
    SequenceNumber snapshot, 
    std::string* value,
    bool* value_found = nullptr);

  void AddIterators(uint32_t cfid,
    const ReadOptions& read_options,
    const EnvOptions& soptions,
    MergeIteratorBuilder* merge_iter_builder);
};


// communication format between rocksdb and Offloader
struct ReplWALUpdate
{
  size_t size;
  SequenceNumber seq;
  char buf[0];
};

enum ReplRequestOp
{
  OP_LOOKUP = 1,
  OP_CURSOR_OPEN = 2,
  OP_CURSOR_NEXT = 3,
  OP_CURSOR_CLOSE = 4,
};

enum ReplResponseOp
{
  RESP_LOOKUP = 1,
  RESP_CURSOR_OPEN = 2,
  RESP_CURSOR_NEXT = 3,
  RESP_CURSOR_CLOSE = 4,
};

struct ReplRequestBase
{
  ReplRequestOp op;
};

struct ReplResponseBase
{
  ReplResponseOp op;
};

struct ReplLookupRequest
{
  ReplRequestOp op = OP_LOOKUP;
  size_t size;
  uint32_t cfid; // column family id
  SequenceNumber seq;
  char buf[0];
};

struct ReplLookupResponse
{
  ReplResponseOp op = RESP_LOOKUP;
  size_t size;
  bool found;
  Status::Code status; 
  char buf[0];
};

typedef uint32_t CursorId;

struct ReplCursorOpen
{
  ReplRequestOp op = OP_CURSOR_OPEN;
  size_t size;
  uint32_t cfid;
  SequenceNumber seq;
  uint32_t numKeysPerNext = 1;
  bool seekFirst = false;
  bool seekLast = false;
  char buf[0]; // has key

  static void* operator new (size_t sz, size_t extra = 0)
  {
    return malloc(sz + extra);
  }
  static void operator delete (void* ptr)
  {
    free(ptr);
  }
};

struct ReplCursorOpenResponse
{
  ReplResponseOp op = RESP_CURSOR_OPEN;
  CursorId cursor_id;  
  Status::Code status;
};

struct ReplCursorNext
{
  ReplRequestOp op = OP_CURSOR_NEXT;
  CursorId cursor_id;
};

struct ReplCursorNextResponse
{
  ReplResponseOp op = RESP_CURSOR_NEXT;
  size_t size;
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;
  char buf[0]; // has <key, seq, type, value> 
};

struct ReplCursorClose
{
  ReplRequestOp op = OP_CURSOR_CLOSE;
  CursorId cursor_id;
};

struct ReplCursorCloseResponse
{
  ReplResponseOp op = RESP_CURSOR_CLOSE;
  CursorId cursor_id;
  Status::Code status;
};

}
