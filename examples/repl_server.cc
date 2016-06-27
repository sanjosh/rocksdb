#include <stdio.h>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cassert>
#include <string.h>
#include <thread> 
#include <map> // inmemory map
#include <string> // key value

#include <rocksdb/db.h>
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
using rocksdb::ColumnFamilyHandle;

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


static constexpr uint32_t kDefaultColumnFamilyIdx = 0; // TODO
static std::string kDBPath = "/tmp/repl_server";

struct DBWrapper {

  std::map<uint32_t, rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::DB* rocksdb_;

  void init(bool newInstance)
  {
    rocksdb::Options options;
    if (newInstance) {
      options.create_if_missing = true;
      options.error_if_exists = false;
    }

    auto s = rocksdb::DB::Open(options, kDBPath, &rocksdb_);
    assert(s.ok());

    handles_.insert(std::make_pair(kDefaultColumnFamilyIdx,
      rocksdb_->DefaultColumnFamily()));
    // TODO need to open all existing cf if not new instance
  }

  rocksdb::ColumnFamilyHandle* openHandle(uint32_t cfid)
  {
    rocksdb::ColumnFamilyHandle* cfptr{nullptr};
    auto iter = handles_.find(cfid);
    if (iter == handles_.end()) {
      auto status = rocksdb_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
        std::to_string(cfid), &cfptr);
      if (status.ok()) {
        handles_.insert(std::make_pair(cfid, cfptr));
      } else {
        std::cerr << "failed to create cf= " << cfid;
      }
    }
    cfptr = iter->second;
    return cfptr;
  }

};

DBWrapper db;

struct MapInserter : public WriteBatch::Handler {

  DBWrapper& db_;
  SequenceNumber seq_;

  MapInserter() = delete;

  explicit MapInserter(SequenceNumber seq, 
    DBWrapper& db) 
    : db_(db), seq_(seq) {}

  virtual Status PutCF(uint32_t cfid, 
    const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf=" << cfid 
      << ":key=" << key.ToString()
      << std::endl;

    auto cf = db_.openHandle(cfid);

    MemKey k(seq_, key.ToString());
    auto status = db_.rocksdb_->Put(rocksdb::WriteOptions(), cf, key, value);

    return status;
  }

  virtual void Put(const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;

    MemKey k(seq_, key.ToString());
    auto status = db.rocksdb_->Put(rocksdb::WriteOptions(), key, value);
    assert(status.ok());
  }

  virtual Status DeleteCF(uint32_t cfid, 
    const Slice& key)
  {
    std::cout << "deleting from cf=" << cfid
      << ":key=" << key.ToString()
      << std::endl;

    auto cf = db_.openHandle(cfid);

    MemKey k(seq_, key.ToString());
    db.rocksdb_->Delete(rocksdb::WriteOptions(), cf, rocksdb::Slice(key.data(), key.size()));

    return Status::OK();
  }

  virtual void Delete(const Slice& key)
  {
    std::cout << "deleting from cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;

    MemKey k(seq_, key.ToString());
    db.rocksdb_->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key.data(), key.size()));
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
    std::string lookupKey(req->key, req->size);
    uint32_t cfid = req->cfid;

    std::cout 
      << "read from ReadSocket size=" << readSz 
      << ":cfid=" << cfid
      << ":key=" << lookupKey
      << ":db map size=" << db.handles_.size()
      << ":seq=" << req->seq
      << std::endl;

    ReplLookupResponse* resp = nullptr;
    size_t totalSz = sizeof(*resp);

    auto cf = db.openHandle(cfid);
    if (cf != nullptr)
    {
      std::string value;
      auto s = db.rocksdb_->Get(rocksdb::ReadOptions(), cf, lookupKey, &value);
      if (s.ok()) 
      {
        std::cout << "found value=" << value << " for key=" << lookupKey << std::endl;

        totalSz += value.size();
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = true;
        memcpy(resp->value, value.data(), value.size());
        resp->size = value.size();
        resp->status = Status::Code::kOk;
      }
      else 
      {
        // key not found
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = false;
        resp->size = 0;
        resp->status = Status::Code::kOk;
        std::cout << "not finding key=" << lookupKey << std::endl;
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
  bool newInstance = false;
  if (argc > 1) {
    newInstance = true;
  }
  db.init(newInstance);

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

  return 0;
}
