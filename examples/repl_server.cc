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
using rocksdb::ReplRequestHeader;
using rocksdb::ReplResponseHeader;
using rocksdb::ReplDatabaseInit;
using rocksdb::ReplDatabaseResp;
using rocksdb::ReplLookupResponse;
using rocksdb::ReplLookupRequest;
using rocksdb::ReplRequestOp;
using rocksdb::ReplCursorOpen;
using rocksdb::ReplCursorOpenResponse;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::ValueType;
using rocksdb::SequenceNumber;
using rocksdb::ColumnFamilyHandle;

/**
 * Store database GUID <-> last_seq, cf
 *
 * initial handshake protocol
 *
 * if (existing guid)
 * {
 *   verify cf list is same
 *   send back last_seq to propagate from
 * }
 * else if rocksdb is initializing
 * {
 *   send back last_seq + cf_list
 *   keep delta = my_seq - client_seq 
 *   (during each wal update, set server_seq = client_seq + delta)
 *   (other option is to reset seqnum on client)
 * }
 *
 * Keep seqnumber with key or with cf/db ?
 *
 * Add custom comparator 
 *
 * Only apply ops greater than seqnum
 */

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
  rocksdb::DB* rocksdb_{nullptr};
  SequenceNumber seq_{0}; // until which upstream rocksdb is synced
  std::string identity_; // guid of upstream rocksdb instance

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

static constexpr int walPort = 8192;

int processLookup(int sockfd, ReplLookupRequest* req, int extraSz)
{
  std::string lookupKey(req->key, extraSz);
  uint32_t cfid = req->cfid;

  std::cout 
    << "read from ReadSocket size=" << extraSz
    << ":cfid=" << cfid
    << ":key=" << lookupKey
    << ":db map size=" << db.handles_.size()
    << ":seq=" << req->seq
    << std::endl;

  ReplLookupResponse* resp = nullptr;
  size_t totalSz = sizeof(*resp);

  auto cf = db.openHandle(cfid);

  int ret = 0;

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
      resp->status = Status::Code::kOk;
    }
    else 
    {
      // key not found
      resp = (ReplLookupResponse*)malloc(totalSz);
      resp->found = false;
      resp->status = Status::Code::kOk;
      std::cout << "not finding key=" << lookupKey << std::endl;
    }
  } 
  else 
  {
    resp = (ReplLookupResponse*)malloc(totalSz);
    resp->status = Status::Code::kInvalidArgument;
    std::cout << "not found cf=" << cfid << std::endl;
  }

  ReplResponseHeader header;
  header.op = rocksdb::ReplResponseOp::RESP_LOOKUP;
  header.size = totalSz;

  do {
    ssize_t writeSz = 0;

    writeSz = write(sockfd, (const void*)&header, sizeof(header));
    if (writeSz != sizeof(header)) {
      std::cout << "response header failed " << writeSz << std::endl;
      ret = -1;
      break;
    }
  
    writeSz = write(sockfd, (const void*)resp, totalSz);
    if (writeSz != totalSz) {
      std::cout << "response header failed " << writeSz << std::endl;
      ret = -1;
      break;
    }

  } while (0);

  free(resp);

  return ret;
}

int processWAL(int sockfd, ReplWALUpdate* req, size_t extraSz)
{
  int ret = 0;

  rocksdb::WriteBatch batch;
  rocksdb::Slice slice(req->buf, extraSz);
  // set contents of batch using Slice
  rocksdb::WriteBatchInternal::SetContents(&batch, slice);

  std::cout << "Got a WriteBatch"
    << " seq=" << req->seq
    << ":size=" << batch.GetDataSize()
    << ":num updates in batch=" << batch.Count()
    << std::endl;

  MapInserter handler(req->seq, db);
  batch.Iterate(&handler);

  return ret;
}

/**
 * handshake protocol cases to handle
 *
 * 1. fresh rocksdb <-> old offloader
 *    offloader.guid is non-null and not equal rocksdb.guid
 *    offloader.seq > rocksdb.seq and rocksdb.seq == 0
 *       offloader sends back seq = 0
 *       offloader adds delta to each received log
 *
 * 2. fresh rocksdb <-> fresh offloader
 *    offloader.guid will be null
 *    offloader.seq = rocksdb.seq = 0
 *
 * 3. old rocksdb   <-> old offloader
 *    if guid mismatch error
 *    offloader.guid is non-null and equal rocksdb.guid
 *    offloader.seq < rocksdb.seq and exists rocksdb wal < offloader.seq
 *       offloader sends back seq from which to transmit
 *    
 * 4. old rocksdb   <-> fresh offloader 
 *    offloader.guid is null and not equal rocksdb.guid
 *    how to sync offloader if some logs were already deleted ?
 *    maybe we dont want to handle this case ?
 */
int processInit(int sockfd, ReplDatabaseInit* req, size_t extraSz)
{
  int ret = 0;

  std::string remoteIdentity(req->identity, req->identitySize);

  const bool IsNewOffloader = ((db.identity_.size() == 0) && (db.seq_ == 0));
  // TODO Pass exact state from client instead of deriving the situation here
  const bool IsNewRocksDB = (req->seq == 0);
  const bool IsDifferent = (db.identity_ != remoteIdentity);

  SequenceNumber responseSeq{0};
  std::string responseIdentity;
  int32_t responseCode = -1;

  std::cout << "handshake got seq=" << req->seq
    << ":identity=" << remoteIdentity
    << std::endl;

  std::string caseString;

  if (IsNewRocksDB)
  {
    assert(remoteIdentity.size());

    if (IsNewOffloader)
    {
      // CASE : fresh rocksdb connecting to fresh offloader
      assert(db.seq_ == 0);
      assert(req->seq == 0);
      db.identity_ = remoteIdentity;
      caseString = "fresh->fresh";
      responseCode = 0;
    }
    else 
    {
      assert(IsDifferent);
      // CASE : fresh rocksdb connecting to old offloader
      // guids are different
      // TODO : maintain delta if we start storing seq with keys on offloader
      db.seq_ = req->seq;
      responseSeq = req->seq;
      responseCode = 0;
      caseString = "fresh->old";
      // send back zero seq num and our identity
      // which rocksdb updates in its DBImpl
    }
  }
  else 
  {
    if (IsNewOffloader) 
    {
      // CASE : old rocksdb connecting to fresh offloader
      // error
      caseString = "old->fresh";
    }
    else 
    {
      assert (db.seq_ != 0);
      assert (req->seq != 0);
      if (db.seq_ <= req->seq) 
      {
        // CASE : old rocksdb re-connecting to old offloader
        responseSeq = db.seq_;
        db.identity_ = remoteIdentity;
        responseCode = 0;
        caseString = "old->old";
      } 
      else
      {
       // CASE : rocksdb has deleted logs which offloader needs!
       // error
        caseString = "very_old->fresh";
      }
    }
  }

  responseIdentity = db.identity_;

  std::cout << "handshake sending seq=" << responseSeq
    << ":identity=" << responseIdentity
    << ":code=" << responseCode
    << ":case=" << caseString
    << std::endl;

  ReplDatabaseResp* resp = nullptr;
  size_t totalSz = sizeof(*resp) + responseIdentity.size();
  resp = (ReplDatabaseResp*)malloc(totalSz);
  resp->identitySize = responseIdentity.size();
  resp->seq = responseSeq;
  memcpy(resp->identity, responseIdentity.data(), responseIdentity.size());


  ReplResponseHeader header;
  header.op = rocksdb::ReplResponseOp::RESP_INIT1;
  header.size = totalSz;

  do {
    ssize_t writeSz = 0;

    writeSz = write(sockfd, (const void*)&header, sizeof(header));
    if (writeSz != sizeof(header)) {
      std::cout << "response header failed " << writeSz << std::endl;
      ret = -1;
      break;
    }
  
    writeSz = write(sockfd, (const void*)resp, totalSz);
    if (writeSz != totalSz) {
      std::cout << "response header failed " << writeSz << std::endl;
      ret = -1;
      break;
    }

  } while (0);

  free(resp);

  return ret;
}

int main(int argc, char* argv[])
{
  bool newInstance = false;
  if (argc > 1) {
    newInstance = true;
  }
  db.init(newInstance);

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

  addr_size = sizeof (serverStorage);
  newSocket = accept(listenSocket, 
    (struct sockaddr *) &serverStorage, &addr_size);

  bool eof = false;

  while (!eof) 
  {

    ReplRequestHeader header;
    ssize_t readSz = read(newSocket, &header, sizeof(header));
    if (readSz != sizeof(header))
    {
      eof = true;
      std::cout << __LINE__ << "got eof" << std::endl;
      break;
    } 

    switch (header.op) 
    {
      case rocksdb::ReplRequestOp::OP_INIT1 : 
      {
        ReplDatabaseInit* req = (ReplDatabaseInit*)malloc(header.size);
        readSz = read(newSocket, req, header.size);
        if (readSz != header.size) 
        {
          eof = true;
          std::cout << __LINE__ 
            << "got sz="  << readSz 
            << " expected=" << header.size 
            << std::endl;
          break;
        }

        int ret = processInit(newSocket, req, header.size - sizeof(*req));
        free(req);
        if (ret != 0) 
        {
          eof = true;
          std::cout << "got handshake error" << std::endl;
          break;
        }
        break;
      }
      case rocksdb::ReplRequestOp::OP_LOOKUP : 
      {

        ReplLookupRequest* req = (ReplLookupRequest*)malloc(header.size);
        readSz = read(newSocket, req, header.size);
        if (readSz != header.size) 
        {
          eof = true;
          std::cout << __LINE__ 
            << "got sz="  << readSz 
            << " expected=" << header.size 
            << std::endl;
          break;
        }

        int ret = processLookup(newSocket, req, header.size - sizeof(*req));
        free(req);
        if (ret != 0) 
        {
          eof = true;
          std::cout << "got lookup error" << std::endl;
          break;
        }
        break;
      }

      case rocksdb::ReplRequestOp::OP_WAL :
      {

        ReplWALUpdate* sw = (ReplWALUpdate*)malloc(header.size);
        readSz = read(newSocket, sw, header.size);
        if (readSz != header.size) 
        {
          eof = true;
          std::cout << __LINE__ 
            << "got sz="  << readSz 
            << " expected=" << header.size 
            << std::endl;
          break;
        }

        int ret = processWAL(newSocket, sw, header.size - sizeof(*sw));
        free(sw);
        if (ret != 0) 
        {
          eof = true;
          std::cout << "got lookup error" << std::endl;
          break;
        }
        break;
      }

      default:
      {
        std::cout << " UKNOWN error" << std::endl;
        eof = true;
      }
    }
  }

  close(listenSocket);
  close(newSocket);

  return 0;
}
