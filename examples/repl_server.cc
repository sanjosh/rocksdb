#include <stdio.h>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <thread> 
#include <map> // inmemory map
#include <string> // key value

#include "rocksdb/types.h" // SequenceNumber
#include "rocksdb/status.h" // Status
#include "rocksdb/slice.h" // Slice
#include "rocksdb/write_batch.h" // WriteBatch
#include "db/version_edit.h" // VersionEdit
#include "db/write_batch_internal.h" // WriteBatchInternal
#include "db/dbformat.h" // ValueType
#include "db/db_repl.h" // Repl structures

using rocksdb::WriteBatch;
using rocksdb::ReplWALUpdate;
using rocksdb::ReplLookupResponse;
using rocksdb::ReplLookupRequest;
using rocksdb::ReplCursorOpen;
using rocksdb::ReplCursorOpenResponse;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::ValueType;
using rocksdb::SequenceNumber;

struct MemValue 
{
  ValueType type;
  std::string value;

  MemValue(ValueType t, const std::string& v)
    : type(t), value(v) {}

  MemValue(ValueType t)
    : type(t) {}

  friend std::ostream& operator << (std::ostream& os, const MemValue& m)
  {
    os << m.type << ":" << m.value;
    return os;
  }
};

struct MemKey
{
  std::string key;
  SequenceNumber seqnum;

  MemKey(SequenceNumber s, const std::string& k)
    : key(k), seqnum(s) {}

  friend std::ostream& operator << (std::ostream& os, const MemKey& m)
  {
    os << m.key << ":" << m.seqnum;
    return os;
  }

};

struct MemKeyCompare 
{
  // which is less
  bool operator() (const MemKey& lhs, const MemKey& rhs) const
  {
    if (lhs.key < rhs.key) return true;
    // keys with higher seqnum are placed earlier in scan
    if ((lhs.key == rhs.key) && (lhs.seqnum > rhs.seqnum)) return true;
    return false;
  }
};

typedef std::map<MemKey, MemValue, MemKeyCompare> InMemKV; // key -> value
typedef std::map<uint32_t, InMemKV> CFMap; // cfid -> map

struct LocalCursor
{
  LocalCursor();

  InMemKV::iterator iter;
};

static constexpr uint32_t kDefaultColumnFamilyIdx = 0; // TODO

struct InMemDB
{
  CFMap columnFamilies_;
  std::map<int32_t, LocalCursor*> openCursors_;

  InMemDB()
  {
    InMemKV kv;
    columnFamilies_.insert(std::make_pair(kDefaultColumnFamilyIdx, kv));
  }

  InMemKV& at(uint32_t column_family_id)
  {
    // dont use operator []; it creates entry if one doesnt exist
    return columnFamilies_.at(column_family_id);
  }

  CFMap::iterator find(uint32_t column_family_id)
  {
    return columnFamilies_.find(column_family_id);
  }

  size_t size() const
  {
    return columnFamilies_.size();
  }

  CFMap::iterator end()
  {
    return columnFamilies_.end();
  }

  int32_t createCursor()
  {
    LocalCursor* nc = new LocalCursor;
    int32_t cursor_id = openCursors_.size();
    openCursors_.insert(std::make_pair(cursor_id, nc));
    return cursor_id;
  }

  LocalCursor* getCursor(int32_t cursor_id)
  {
    // dont use operator []; it creates entry if one doesnt exist
    auto iter = openCursors_.find(cursor_id);
    if (iter != openCursors_.end())
    {
      return iter->second;
    }
    return nullptr;
  }
};


InMemDB db;

struct MapInserter : public WriteBatch::Handler {

  InMemDB& db_;
  SequenceNumber seq_;

  MapInserter() = delete;

  explicit MapInserter(SequenceNumber seq, 
    InMemDB& db) 
    : db_(db), seq_(seq) {}

  virtual Status PutCF(uint32_t column_family_id, 
    const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf=" << column_family_id 
      << ":key=" << key.ToString()
      << std::endl;

    auto& kvmap = db_.at(column_family_id);

    MemKey k(seq_, key.ToString());
    MemValue v(rocksdb::ValueType::kTypeColumnFamilyValue, value.ToString());
    kvmap.insert(std::make_pair(k, v));

    return Status::OK();
  }

  virtual void Put(const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;

    auto& kvmap = db_.at(kDefaultColumnFamilyIdx);
    MemKey k(seq_, key.ToString());
    MemValue v(rocksdb::ValueType::kTypeValue, value.ToString());
    kvmap.insert(std::make_pair(k, v));
  }

  virtual Status DeleteCF(uint32_t column_family_id, 
    const Slice& key)
  {
    std::cout << "deleting from cf=" << column_family_id
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_.at(column_family_id);

    MemKey k(seq_, key.ToString());
    MemValue v(rocksdb::ValueType::kTypeColumnFamilyDeletion);
    kvmap.insert(std::make_pair(k, v));

    return Status::OK();
  }

  virtual void Delete(const Slice& key)
  {
    std::cout << "deleting from cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_.at(kDefaultColumnFamilyIdx);

    MemKey k(seq_, key.ToString());
    MemValue v(rocksdb::ValueType::kTypeDeletion);
    kvmap.insert(std::make_pair(k, v));
  }

  virtual void LogData(const Slice& blob)
  {
    rocksdb::VersionEdit edit;
    auto s = edit.DecodeFrom(blob);
    if (s.ok()) {
      std::cout << "got version edit="
        << edit.DebugString() << std::endl;
    }
  }
};

static constexpr size_t MaxReadSize = 8192;
static constexpr int walPort = 8192;
static constexpr int readPort = walPort + 1;


void readWork()
{
  int listenSocket, newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;
  socklen_t addr_size;

  listenSocket = socket(PF_INET, SOCK_STREAM, 0);
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(readPort);
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);  

  bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));

  if(listen(listenSocket,5)==0)
    printf("Listening on %d\n", readPort);
  else {
    printf("Error\n");
    pthread_exit(0);
  }

  addr_size = sizeof serverStorage;
  newSocket = accept(listenSocket, 
    (struct sockaddr *) &serverStorage, &addr_size);

  char buf[MaxReadSize];
  bool eof = false;

  std::cout << "got new connection to handle reads" << std::endl;

  while (!eof) {
    bzero(buf, sizeof(buf));
    const ssize_t readSz = read(newSocket, buf, sizeof(buf));
    if (readSz <= 0) {
      eof = true;
    }


    ReplLookupRequest* req = reinterpret_cast<ReplLookupRequest*>(buf);
    MemKey lookupKey(req->seq, std::string(req->buf, req->size));
    uint32_t cfid = req->cfid;

    std::cout 
      << "read from ReadSocket size=" << readSz 
      << ":cfid=" << cfid
      << ":key=" << lookupKey.key
      << ":db map size=" << db.size()
      << ":seq=" << req->seq
      << std::endl;

    ReplLookupResponse* resp = nullptr;
    size_t totalSz = sizeof(*resp);

    auto iter = db.find(cfid);
    if (iter != db.end())
    {
      auto& kvmap = iter->second;
      // keys are ordered by higher seqnum to lower
      auto it = kvmap.lower_bound(lookupKey);
      MemKey endKey(0 , std::string(req->buf, req->size));
      auto end_iter = kvmap.upper_bound(endKey);
      for (; it != end_iter; it ++)
      {
        std::cout << "found value=" << it->second << " for key=" << lookupKey << std::endl;

        // CHECK IF key matches requested key
        MemValue& val = it->second;

        // TODO process ValueType = delete
        totalSz += val.value.size();
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = ((val.type != rocksdb::ValueType::kTypeDeletion) && (val.type != rocksdb::ValueType::kTypeColumnFamilyDeletion));
        memcpy(resp->buf, val.value.data(), val.value.size());
        resp->size = val.value.size();
        resp->status = Status::Code::kOk;
        break;
      }
      if (resp == nullptr)
      {
        // key not found
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = false;
        resp->size = 0;
        resp->status = Status::Code::kOk;
        std::cout << "iter not finding key=" << lookupKey << std::endl;
      }
    } 
    else 
    {
      resp = (ReplLookupResponse*)malloc(totalSz);
      resp->size = 0;
      resp->status = Status::Code::kInvalidArgument;
      std::cout << "not found cf=" << cfid << std::endl;
    }

    const ssize_t writeSz = write(newSocket, (const void*)resp, totalSz);
    if (writeSz != totalSz) {
      std::cout << "response failed in readWork" << std::endl;
      eof = true;
    }
  }

  close(newSocket);
  close(listenSocket);
}

/*
void scanWork()
{
  int listenSocket, newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;
  socklen_t addr_size;

  listenSocket = socket(PF_INET, SOCK_STREAM, 0);
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(readPort);
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);  

  bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));

  if(listen(listenSocket,5)==0)
    printf("Listening on %d\n", readPort);
  else {
    printf("Error\n");
    pthread_exit(0);
  }

  addr_size = sizeof serverStorage;
  newSocket = accept(listenSocket, 
    (struct sockaddr *) &serverStorage, &addr_size);

  char buf[MaxReadSize];
  bool eof = false;

  std::cout << "got new connection to handle reads" << std::endl;

  while (!eof) {

    bzero(buf, sizeof(buf));
    const ssize_t readSz = read(newSocket, buf, sizeof(buf));
    if (readSz <= 0) {
      eof = true;
    }

    ReplCursorOpen* req = reinterpret_cast<ReplCursorOpen*>(buf);
    MemKey lookupKey(req->seq, std::string(req->buf, req->size));
    uint32_t cfid = req->cfid;

    std::cout 
      << "read from ScanSocket size=" << readSz 
      << ":cfid=" << cfid
      << ":key=" << lookupKey.key
      << ":db map size=" << db.size()
      << ":seq=" << req->seq
      << std::endl;

    ReplCursorOpenResponse* resp = nullptr;
    size_t totalSz = sizeof(*resp);

    auto iter = db.find(cfid);
    if (iter != db.end())
    {
      auto& kvmap = iter->second;
      auto it = kvmap.lower_bound(lookupKey);
      for (; it != kvmap.end() ; ++ it )
      {
        std::cout << "found value=" << it->second << " for key=" << lookupKey << std::endl;

        // TODO process ValueType = delete
        totalSz += val.value.size();
        resp = (ReplCursorOpenResponse*)malloc(totalSz);
        resp->found = ((val.type != rocksdb::ValueType::kTypeDeletion) && (val.type != rocksdb::ValueType::kTypeColumnFamilyDeletion));
        memcpy(resp->buf, val.value.data(), val.value.size());
        resp->size = val.value.size();
        resp->status = Status::Code::kOk;
        break;
      }
      if (resp == nullptr)
      {
        // key not found
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = false;
        resp->size = 0;
        resp->status = Status::Code::kOk;
        std::cout << "iter not finding key=" << lookupKey << std::endl;
      }
    } 
    else 
    {
      resp = (ReplLookupResponse*)malloc(totalSz);
      resp->size = 0;
      resp->status = Status::Code::kInvalidArgument;
      std::cout << "not found cf=" << cfid << std::endl;
    }

    const ssize_t writeSz = write(newSocket, (const void*)resp, totalSz);
    if (writeSz != totalSz) {
      std::cout << "response failed in readWork" << std::endl;
      eof = true;
    }
  }

  close(newSocket);
  close(listenSocket);
}
*/

int main(int argc, char* argv[])
{
  std::thread readThr = std::thread(readWork);

  int listenSocket, newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;
  socklen_t addr_size;

  listenSocket = socket(PF_INET, SOCK_STREAM, 0);
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(walPort);
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);  

  bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));

  if(listen(listenSocket,5)==0)
    printf("Listening on %d\n", walPort);
  else {
    printf("Error\n");
    exit(1);
  }

  addr_size = sizeof serverStorage;
  newSocket = accept(listenSocket, 
    (struct sockaddr *) &serverStorage, &addr_size);

  char buf[MaxReadSize];
  off_t processedOff = 0;
  off_t readOff = 0;
  bool eof = false;

  bzero(buf, sizeof(buf));

  while (!eof) {

    // read from socket until eof
    ssize_t readSz = read(newSocket, buf + readOff, sizeof(buf));
    if (readSz <= 0) {
      eof = true;
    } else {
      readOff += readSz;
    }

    std::cout 
      << "read from WriteSocket size=" << readSz 
      << ":processedOff=" << processedOff 
      << ":readOff=" << readOff 
      << std::endl;

    // for now,
    // assume multiple records are read from socket
    // no record gets fragmented during read
    while (processedOff < readOff) {

      // assert that readOff - processedOff > value being read
      ReplWALUpdate* sw
        = (ReplWALUpdate*)(buf + processedOff);

      if (sw->size <= (readOff - processedOff - sizeof(ReplWALUpdate))) {

        rocksdb::WriteBatch batch;
        rocksdb::Slice slice(sw->buf, sw->size);
        // set contents of batch using Slice
        rocksdb::WriteBatchInternal::SetContents(&batch, slice);
  
        std::cout << "Got a WriteBatch"
          << " seq=" << sw->seq
          << ":size=" << batch.GetDataSize()
          << ":num updates in batch=" << batch.Count()
          << std::endl;

        processedOff += sizeof(ReplWALUpdate) + sw->size;

        MapInserter handler(sw->seq, db);
        batch.Iterate(&handler);
        
      } else {
        std::cout << "fragmented record read" << std::endl;
        eof = true;
        break;
      }
    }

    readOff = processedOff = 0;
  }

  close(listenSocket);
  close(newSocket);

  std::cout << "Printing in-memory db" << std::endl;
  for (auto kvmap : db.columnFamilies_)
  {
    for (auto elem : kvmap.second)
    {
      std::cout 
        << "key=" << elem.first 
        << ":value=" << elem.second 
        << std::endl;
    }
  }

  return 0;
}
