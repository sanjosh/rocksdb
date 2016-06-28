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

  SequenceNumber lastReplSequence;

  void walUpdater();

  int initialize(const std::string& guid,
      SequenceNumber lastSequence);

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
  OP_INIT1 = 1,
  OP_WAL = 101,
  OP_LOOKUP = 201,
  OP_CURSOR_OPEN = 202,
  OP_CURSOR_NEXT = 203,
  OP_CURSOR_CLOSE = 204,
};

enum ReplResponseOp
{
  RESP_INIT1 = 1,
  // add for wal?
  RESP_LOOKUP = 201,
  RESP_CURSOR_OPEN = 202,
  RESP_CURSOR_NEXT = 203,
  RESP_CURSOR_CLOSE = 204,
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

// send different message for new db or existing db
struct ReplDatabaseInit
{
  SequenceNumber seq;
  size_t identitySize;
  char identity[0];
  // TODO send list of column families
};

struct ReplDatabaseResp
{
  SequenceNumber seq;
  size_t identitySize;
  char identity[0];
  // TODO send back list of column families
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
