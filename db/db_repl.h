#pragma once

#include "rocksdb/types.h"
#include "rocksdb/status.h"
#include <string>
#include <atomic>
#include <list>

#include <vector>
#include <queue>
#include <cassert>
#include <string.h>

namespace rocksdb {

class Logger;
class DBImpl;
class MergeIteratorBuilder;
class EnvOptions;
class ReadOptions;
class ColumnFamilyHandle;
class Slice;

struct free_delete
{
  void operator()(void* x) { std::free(x); }
};

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
  OP_CURSOR_MULTI_NEXT = 204,
  OP_CURSOR_CLOSE = 205,

  RESP_INIT1 = RESP_BEGIN + OP_INIT1,
  // add for wal?
  RESP_LOOKUP = RESP_BEGIN + OP_LOOKUP,

  RESP_CURSOR_OPEN = RESP_BEGIN + OP_CURSOR_OPEN,
  RESP_CURSOR_NEXT = RESP_BEGIN + OP_CURSOR_NEXT,
  RESP_CURSOR_MULTI_NEXT = RESP_BEGIN + OP_CURSOR_MULTI_NEXT,
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

  int writeSocket(ReplRequestOp op, const void* data, const size_t totalSz, SequenceNumber seq);

  int readSocket(ReplResponseOp& op, void** data, ssize_t &returnSz, SequenceNumber* seq = nullptr);

  int close();

  bool IsOpen() const;

  explicit ReplSocket();

  explicit ReplSocket(int sockfd);

  ~ReplSocket();
};

/**
 * This is the key format as stored on the offloader
 * Either in the key or value, we must retain the
 * original sequence number and ValueType so
 * it can be returned to the client-side rocksdb 
 * during iteration or lookup
 */
struct ReplKey
{
  // Current format
  // SequenceNumber - 8 byte
  // ValueType - 4 byte
  // Length of string 
  // Actual string
  //
  // Should put seq and val in the back of the string
  // rather than front
  std::string rep;

  ReplKey(const std::string& key, SequenceNumber seqnum, ValueType value)
  {
    PutFixed64(&rep, seqnum);
    PutFixed32(&rep, value);
    rep.append(key);
  }

  ReplKey(const rocksdb::Slice& key, SequenceNumber seqnum, ValueType value)
  {
    PutFixed64(&rep, seqnum);
    PutFixed32(&rep, value);
    rep.append(key.data(), key.size());
  }

  ReplKey(const Slice& s) 
  {
    assert(s.size() > sizeof(uint64_t) + sizeof(uint32_t));
    rep.assign(s.data(), s.size());
  }

  ReplKey(const std::string& s) 
  {
    assert(s.size() > sizeof(uint64_t) + sizeof(uint32_t));
    rep.assign(s.data(), s.size());
  }

  SequenceNumber seq() const
  {
    Slice s(rep.data(), sizeof(uint64_t));
    SequenceNumber ret;
    GetFixed64(&s, &ret);
    return ret;
  }

  std::string userKey() const
  {
    return std::string(rep.data() + sizeof(uint64_t) + sizeof(uint32_t), 
      rep.size() - (sizeof(uint64_t) + sizeof(uint32_t)));
  }

  ValueType val() const
  {
    Slice s(rep.data() + sizeof(uint64_t), sizeof(uint32_t));
    uint32_t value;
    GetVarint32(&s, &value);
    return static_cast<ValueType>(value);
  }

  Slice Encode() const
  {
    return rep;
  }
};

struct ReplKeyComparator : public rocksdb::Comparator 
{
  virtual int Compare(const Slice& as, const Slice& bs) const override
  {
    ReplKey a(as);
    ReplKey b(bs);

    auto akey = a.userKey();
    auto bkey = b.userKey();

    int cmp = akey.compare(bkey);
    if (cmp < 0) {
      return -1;
    } else if (cmp > 0) {
      return 1;
    } else {
      return 0;
    }
  }

  virtual const char* Name() const override
  {
    return "repl-rocks";
  }

  virtual void FindShortestSeparator(
    std::string* start,
    const Slice& limit) const override
  {
  }

  virtual void FindShortSuccessor(std::string* key) const override
  {
  }

};


struct ReplThreadInfo {

  rocksdb::DBImpl* db = nullptr;
  std::shared_ptr<rocksdb::Logger> info_log = nullptr;

  std::atomic<bool> stop{false}; // replace by cv
  std::atomic<bool> has_stopped{false};
  std::atomic<bool> started{false};

  // writeSock is where all wal updates are written
  ReplSocket writeSock;
  // Introduced read socket because currently there
  // is a mutex used to guarantee exclusive use of
  // socket to every operation.  This caused a deadlock
  // TODO remove mutex and put in smarter socket multiplexing 
  ReplSocket readSock;

  // last seq shipped to offloader
  SequenceNumber lastReplSequence{0};

  // last seq acked by offloader
  SequenceNumber lastAckedSequence{0};

  std::list<std::string> replLogList;

  void walUpdater();

  // for synchronous commit
  Status AddToReplLog(WriteBatch& newBatch);
  Status FlushReplLog();

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
    return (writeSock.IsOpen() && readSock.IsOpen());
  }
};

/*
 * manages serialization buffer
 */
struct BufferIter
{
  std::vector<char> buffer;
  size_t offset{0};

  static constexpr size_t InitialAllocSize = 1024;

  BufferIter(char* buf, size_t insz);
  virtual ~BufferIter();

  int readNext(char* outbuf, size_t outsz);
  int writeNext(const char* inbuf, size_t insz);

  int readQueue(size_t numEnt, std::queue<std::string>& stringArray);
  ssize_t writeVector(const std::vector<std::string>& stringArray);

  const char* data() const { return buffer.data(); }
  size_t size() const { return offset; }
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
  // send local seq number to other side
  SequenceNumber seq; 
  size_t size;
};

// TODO separate these two headers later
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
  int direction{0};
  SequenceNumber seq;
  KeyValue kv;
};

struct ReplCursorNextReq
{
  CursorId cursor_id;
  // 1 for next, -1 for prev
  int direction{0}; 
};

struct ReplCursorNextResp
{
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;
  KeyValue kv;
};

struct ReplCursorMultiNextReq
{
  CursorId cursor_id;
  // max entries to send in the response buffer
  uint32_t num_requested;
  // 1 for next, -1 for prev
  int direction{0}; 
};

struct ReplCursorMultiNextResp
{
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;

  // number of entries in each of 2 arrays serialized below
  uint32_t num_sent; 

  size_t keySz;
  size_t valueSz;
  char buf[0];
  // followed by
  // array of keys of byte size=keySz
  // array of values of byte size=valueSz
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
