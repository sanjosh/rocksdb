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

  std::mutex sock_mutex;

  int socket = -1; // used to send WAL to offloader
  int port = -1;
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

  bool IsReplicated() const
  {
    return (socket != -1);
  }
};


// communication format between rocksdb and Offloader
enum ReplRequestOp
{
  OP_LOOKUP = 1,
  OP_CURSOR_OPEN = 2,
  OP_CURSOR_NEXT = 3,
  OP_CURSOR_CLOSE = 4,
  OP_WAL = 5,
};

enum ReplResponseOp
{
  RESP_LOOKUP = 1,
  RESP_CURSOR_OPEN = 2,
  RESP_CURSOR_NEXT = 3,
  RESP_CURSOR_CLOSE = 4,
};

struct ReplRequestHeader
{
  ReplRequestOp op;
  size_t size;
};

struct ReplResponseHeader
{
  ReplResponseOp op;
  size_t size;
};

struct ReplLookupRequest
{
  uint32_t cfid; // column family id
  SequenceNumber seq;
  char key[0];
};

struct ReplLookupResponse
{
  bool found;
  Status::Code status; 
  char value[0];
};

struct ReplWALUpdate
{
  SequenceNumber seq;
  char buf[0];
};

typedef uint32_t CursorId;

struct ReplCursorOpen
{
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
  CursorId cursor_id;  
  Status::Code status;
};

struct ReplCursorNext
{
  CursorId cursor_id;
};

struct ReplCursorNextResponse
{
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;
  char buf[0]; // has <key, seq, type, value> 
};

struct ReplCursorClose
{
  CursorId cursor_id;
};

struct ReplCursorCloseResponse
{
  CursorId cursor_id;
  Status::Code status;
};

}
