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


#define RESP_BEGIN 1000
// communication format between rocksdb and Offloader
enum ReplRequestOp
{
  OP_WILDCARD = 0,

  OP_INIT1 = 1,

  OP_WAL = 101,

  OP_LOOKUP = 201,

  OP_CURSOR_OPEN = 202,
  OP_CURSOR_NEXT = 203,
  OP_CURSOR_CLOSE = 204,

  RESP_INIT1 = RESP_BEGIN + OP_INIT1,
  // add for wal?
  RESP_LOOKUP = RESP_BEGIN + OP_LOOKUP,

  RESP_CURSOR_OPEN = RESP_BEGIN + OP_CURSOR_OPEN,
  RESP_CURSOR_NEXT = RESP_BEGIN + OP_CURSOR_NEXT,
  RESP_CURSOR_CLOSE = RESP_BEGIN + OP_CURSOR_CLOSE,
};

typedef ReplRequestOp ReplResponseOp;


struct ReplSocket
{
  int sock_fd{-1};
  int port{-1};
  std::string addr;
  std::mutex sock_mutex; // dont need it on server-offloader size
  std::shared_ptr<rocksdb::Logger> logger = nullptr;

  int connect(const std::string& in_addr, int in_port,
      std::shared_ptr<rocksdb::Logger> in_logger);

  int writeSocket(ReplRequestOp op, const void* data, const size_t totalSz);

  int readSocket(ReplResponseOp& op, void** data, ssize_t &returnSz);

  explicit ReplSocket();

  explicit ReplSocket(int sockfd);

  ~ReplSocket();
};

struct ReplThreadInfo {

  rocksdb::DBImpl* db = nullptr;
  std::shared_ptr<rocksdb::Logger> info_log = nullptr;

  std::atomic<bool> stop; // replace by cv
  std::atomic<bool> has_stopped;
  std::atomic<bool> started;

  // writeSock is where all wal updates are written
  ReplSocket writeSock;
  // Introduced read socket because currently there
  // is a mutex used to guarantee exclusive use of
  // socket to every operation.  This caused a deadlock
  // TODO remove mutex and put in smarter socket multiplexing 
  ReplSocket readSock;

  SequenceNumber lastReplSequence{0};

  void walUpdater();

  int initialize(const std::string& guid,
      SequenceNumber lastSequence,
      const std::string& addr,
      int port);

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
    return ((writeSock.sock_fd != -1) && (readSock.sock_fd != -1));
  }
};

struct KeyValue
{
  uint32_t key_size{0};
  uint32_t value_size{0};
  char buf[0];

  static KeyValue* create(uint32_t key_size, uint32_t value_size)
  {
    KeyValue* kv = reinterpret_cast<KeyValue*>(malloc(key_size + value_size + sizeof(KeyValue)));
    kv->key_size = key_size;
    kv->value_size = value_size;
    return kv;
  }

  static void destroy(KeyValue* kv)
  {
    free(kv);
  }

  std::string getKey() const
  {
    std::string s(buf, key_size);
    return s;
  }

  std::string getValue() const
  {
    std::string s(buf + key_size, value_size);
    return s;
  }

  void putKey(const Slice& key)
  {
    key_size = key.size();
    memcpy(buf, key.data(), key_size);
  }

  void putValue(const Slice& value)
  {
    value_size = value.size();
    memcpy(buf + key_size, value.data(), value_size);
  }
};

struct ReplRequestHeader
{
  ReplRequestOp op;
  size_t size;
};

typedef ReplRequestHeader ReplResponseHeader;

// send different message for new db or existing db
struct ReplDBReq
{
  SequenceNumber seq;
  size_t identitySize;
  char identity[0];
  // TODO send list of column families
};

struct ReplDBResp
{
  SequenceNumber seq;
  size_t identitySize;
  char identity[0];
  // TODO send back list of column families
};

struct ReplLookupReq
{
  uint32_t cfid; // column family id
  SequenceNumber seq;
  char key[0];
};

struct ReplLookupResp
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

struct ReplCursorOpenReq
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


struct ReplCursorOpenResp
{
  CursorId cursor_id;  
  Status::Code status;
  bool is_eof;
  SequenceNumber seq;
  KeyValue kv;
};

struct ReplCursorNextReq
{
  CursorId cursor_id;
};

struct ReplCursorNextResp
{
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;
  SequenceNumber seq;
  KeyValue kv;
};

struct ReplCursorCloseReq
{
  CursorId cursor_id;
};

struct ReplCursorCloseResp
{
  CursorId cursor_id;
  Status::Code status;
};

}
