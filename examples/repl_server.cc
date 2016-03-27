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
#include "db/dbformat.h" // SequenceNumber

using rocksdb::WriteBatch;
using rocksdb::ReplServerBlock;
using rocksdb::ReplLookupResponse;
using rocksdb::ReplLookupRequest;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::ValueType;
using rocksdb::SequenceNumber;

struct MemValue {
  SequenceNumber seqnum;
  ValueType type;
  std::string value;

  MemValue(SequenceNumber seq, ValueType t, const std::string& v)
    : seqnum(seq), type(t), value(v) {}

  MemValue(SequenceNumber seq, ValueType t)
    : seqnum(seq), type(t) {}

  friend std::ostream& operator << (std::ostream& os, const MemValue& m)
  {
    os << m.seqnum << ":" << m.type << ":" << m.value;
    return os;
  }
};

typedef std::multimap<std::string, MemValue> InMemKV; // key -> value
typedef std::map<uint32_t, InMemKV> InMemDB; // cfid -> map

static constexpr uint32_t kDefaultColumnFamilyIdx = 0; // TODO

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

    auto& kvmap = db_[column_family_id];
    MemValue m(seq_, rocksdb::ValueType::kTypeColumnFamilyValue, value.ToString());
    kvmap.insert(std::make_pair(key.ToString(), m));
    return Status::OK();
  }

  virtual void Put(const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;

    auto& kvmap = db_[kDefaultColumnFamilyIdx];
    MemValue m(seq_, rocksdb::ValueType::kTypeValue, value.ToString());
    kvmap.insert(std::make_pair(key.ToString(), m));
  }

  virtual Status DeleteCF(uint32_t column_family_id, 
    const Slice& key)
  {
    std::cout << "deleting from cf=" << column_family_id
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_[column_family_id];
    MemValue m(seq_, rocksdb::ValueType::kTypeColumnFamilyDeletion);
    kvmap.insert(std::make_pair(key.ToString(), m));
    return Status::OK();
  }

  virtual void Delete(const Slice& key)
  {
    std::cout << "deleting from cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_[kDefaultColumnFamilyIdx];
    MemValue m(seq_, rocksdb::ValueType::kTypeDeletion);
    kvmap.insert(std::make_pair(key.ToString(), m));
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

InMemDB db;
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

  char buf[8192];
  bool eof = false;

  std::cout << "got new connection to handle reads" << std::endl;

  while (!eof) {
    bzero(buf, sizeof(buf));
    const ssize_t readSz = read(newSocket, buf, sizeof(buf));
    if (readSz <= 0) {
      eof = true;
    }


    ReplLookupRequest* req = reinterpret_cast<ReplLookupRequest*>(buf);
    std::string lookupKey(req->buf, req->size);
    uint32_t cfid = req->cfid;

    std::cout 
      << "read from ReadSocket size=" << readSz 
      << ":cfid=" << cfid
      << ":key=" << lookupKey
      << ":db map size=" << db.size()
      << ":seq=" << req->seq
      << std::endl;

    ReplLookupResponse* resp = nullptr;
    size_t totalSz = sizeof(*resp);

    auto iter = db.find(cfid);
    if (iter != db.end())
    {
      auto& kvmap = iter->second;
      auto ret = kvmap.equal_range(lookupKey);
      for (auto it = ret.first; it != ret.second; it++)
      {
        std::cout << "found value=" << it->second << " for key=" << lookupKey << std::endl;

        MemValue& val = it->second;

        // TODO process ValueType = delete
        totalSz += val.value.size();
        resp = (ReplLookupResponse*)malloc(totalSz);
        resp->found = true;
        memcpy(resp->buf, val.value.data(), val.value.size());
        resp->size = val.value.size();
        break;
      }
    } 
    else 
    {
      resp = (ReplLookupResponse*)malloc(totalSz);
      resp->size = 0;
      resp->found = false;
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

  char buf[8192];
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
      ReplServerBlock* sw
        = (ReplServerBlock*)(buf + processedOff);

      if (sw->size <= (readOff - processedOff - sizeof(ReplServerBlock))) {

        rocksdb::WriteBatch batch;
        rocksdb::Slice slice(sw->buf, sw->size);
        // set contents of batch using Slice
        rocksdb::WriteBatchInternal::SetContents(&batch, slice);
  
        std::cout << "Got a WriteBatch"
          << " seq=" << sw->seq
          << ":size=" << batch.GetDataSize()
          << ":num updates in batch=" << batch.Count()
          << std::endl;

        processedOff += sizeof(ReplServerBlock) + sw->size;

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
  for (auto kvmap : db)
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
