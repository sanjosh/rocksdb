#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cassert>
#include <string.h>
#include <thread> 
#include <mutex> 
#include <map> // inmemory map
#include <string> // key value
#include <thread>
#include <future>
#include <signal.h>

#include <rocksdb/db.h>
#include "rocksdb/types.h" // SequenceNumber
#include "rocksdb/status.h" // Status
#include "rocksdb/slice.h" // Slice
#include "rocksdb/write_batch.h" // WriteBatch
#include "db/version_edit.h" // VersionEdit
#include "db/write_batch_internal.h" // WriteBatchInternal
#include "db/dbformat.h" // ValueType and InternalKey
#include "db/db_repl.h" // Repl structures
#include "util/coding.h" // Repl structures
#include "os_util.h"

using rocksdb::WriteBatch;
using rocksdb::ReplSocket;

using rocksdb::ReplWALUpdate;
using rocksdb::ReplRequestHeader;
using rocksdb::ReplResponseHeader;
using rocksdb::ReplDBReq;
using rocksdb::ReplDBResp;
using rocksdb::ReplLookupResp;
using rocksdb::ReplLookupReq;
using rocksdb::ReplRequestOp;
using rocksdb::ReplResponseOp;
using rocksdb::ReplCursorOpenReq;
using rocksdb::ReplCursorOpenResp;

using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::ValueType;
using rocksdb::SequenceNumber;
using rocksdb::ColumnFamilyHandle;

using rocksdb::InternalKey;
using rocksdb::BufferIter;
using rocksdb::LookupKey;

using rocksdb::PutFixed64;
using rocksdb::PutFixed32;
using rocksdb::GetFixed64;
using rocksdb::GetVarint32;

using rocksdb::ReplKey;
using rocksdb::ReplKeyComparator;

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


static constexpr uint32_t kDefaultColumnFamilyIdx = 0; // TODO
static std::string kDBPath = "/tmp/rocksdb_repl_server";

struct DBWrapper {

  std::map<uint32_t, rocksdb::ColumnFamilyHandle*> handles_;
  std::mutex handlesMutex_;

  std::map<uint32_t, rocksdb::Iterator*> openCursors_;

  int32_t nextCursorId_{0};

  rocksdb::DB* rocksdb_{nullptr};

  // seq until which upstream rocksdb is synced
  std::atomic<SequenceNumber> lastAppliedSeq_{0}; 

  std::string identity_; // guid of upstream rocksdb instance

  ~DBWrapper() 
  {
  }

  int init(bool newInstance)
  {
    rocksdb::Status status;

    rocksdb::Options options;
    options.comparator = new ReplKeyComparator();

    if (newInstance) {

      options.create_if_missing = true;
      options.error_if_exists = false;

      status = rocksdb::DB::Open(options, kDBPath, &rocksdb_);

      if (status.ok()) {
        handles_.insert(std::make_pair(kDefaultColumnFamilyIdx,
          rocksdb_->DefaultColumnFamily()));
      }

    } else {

      std::vector<std::string> columnFamilyNames;

      status = rocksdb::DB::ListColumnFamilies(options,
        kDBPath, 
        &columnFamilyNames);

      if (status.ok()) {

        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilyNameVec;
        std::vector<rocksdb::ColumnFamilyHandle*> columnFamilyHandleVec;

        for (auto elem : columnFamilyNames) {
          columnFamilyNameVec.push_back(rocksdb::ColumnFamilyDescriptor(
            elem, rocksdb::ColumnFamilyOptions()));
        }

        status = rocksdb::DB::Open(options, kDBPath, 
            columnFamilyNameVec, 
            &columnFamilyHandleVec, 
            &rocksdb_);

        if (status.ok()) {
          for (auto cfhandle : columnFamilyHandleVec)
          {
            handles_.insert(std::make_pair(cfhandle->GetID(), cfhandle));
          }
        }
      }
    }

    assert(status.ok()); // TODO add error checks later

    return 0;
  }

  // TODO do schema ops when u get createCF/deleteCF blobs in WAL
  rocksdb::ColumnFamilyHandle* openHandle(uint32_t cfid)
  {
    std::unique_lock<std::mutex> l(handlesMutex_);

    rocksdb::ColumnFamilyHandle* cfptr{nullptr};
    auto iter = handles_.find(cfid);
    if (iter == handles_.end()) {
      // TODO this should be done on getting VersionEdit in WAL
      rocksdb::ColumnFamilyOptions column_family_options;
      column_family_options.comparator = new ReplKeyComparator();

      auto status = rocksdb_->CreateColumnFamily(column_family_options,
        std::to_string(cfid), &cfptr);
      if (status.ok()) {
        handles_.insert(std::make_pair(cfid, cfptr));
        std::cout << " thread=" << std::this_thread::get_id() 
          << " create CF= " << cfid 
          << " cfptr = " << cfptr 
          << " seq=" << lastAppliedSeq_ << std::endl;
      } else {
        std::cout << "thread=" << std::this_thread::get_id() 
          << " failed to create CF= " << cfid << std::endl;
      }
    } else {
      cfptr = iter->second;
    }
    return cfptr;
  }

};

DBWrapper db;

// code added to dump the values to find what bson looks like
static int ctr = 0;
static void write_bson(const std::string& value)
{
#ifdef BSON_WRITER
  std::ofstream bsonfile;
  std::string filename = "./bson" + std::to_string(++ctr);
  bsonfile.open(filename);
  bsonfile << value;
  bsonfile.close();
#endif
}

struct MapInserter : public WriteBatch::Handler {

  DBWrapper& db_;

  // Sequence number must be incremented by every op
  // in batch, bcos thats what client-side rocksdb does
  SequenceNumber seq_;

  MapInserter() = delete;

  explicit MapInserter(SequenceNumber seq, 
    DBWrapper& db) 
    : db_(db), seq_(seq) {}

  virtual Status PutCF(uint32_t cfid, 
    const Slice& key,
    const Slice& value) override
  {
    rocksdb::ColumnFamilyHandle* cf {nullptr};

    do {
      cf = db_.openHandle(cfid);
      if (cf == nullptr) {
        usleep(1000);
      }
    } while (cf == nullptr); 

    ReplKey k(key, seq_++, ValueType::kTypeValue);
    rocksdb::Slice kSlice = k.Encode();

    /*
    std::cout << "INSERT cf=" << cfid 
      << ":key=" << kSlice.ToString()
      << ":key_size=" << kSlice.size()
      << ":value=" << std::string(value.data(), 10)
      << std::endl;
      */

    auto status = db_.rocksdb_->Put(rocksdb::WriteOptions(), cf, kSlice, value);

    write_bson(value.ToString());

    return status;
  }

  virtual void Put(const Slice& key,
    const Slice& value) override
  {

    ReplKey k(key, seq_++, ValueType::kTypeValue);
    rocksdb::Slice kSlice = k.Encode();
    
    /*
    std::cout << "INSERT cf="  << kDefaultColumnFamilyIdx
      << ":key=" << kSlice.ToString()
      << ":key_size=" << kSlice.size()
      << ":value=" << std::string(value.data(), 10)
      << std::endl;
      */

    auto status = db.rocksdb_->Put(rocksdb::WriteOptions(), kSlice, value);

    write_bson(value.ToString());

    assert(status.ok());
  }

  virtual Status DeleteCF(uint32_t cfid, 
    const Slice& key)
  {
    rocksdb::ColumnFamilyHandle* cf {nullptr};
    do {
      cf = db_.openHandle(cfid);
      if (cf == nullptr) {
        usleep(1000);
      }
    } while (cf == nullptr); 

    ReplKey k(key, seq_++, ValueType::kTypeDeletion);
    rocksdb::Slice kSlice = k.Encode();
    rocksdb::Slice value;
    /*
    std::cout << "DELETE cf=" << cfid
      << ":key=" << key.ToString()
      << std::endl;
      */

    // insert a tombstone
    db.rocksdb_->Put(rocksdb::WriteOptions(), cf, kSlice, value);

    return Status::OK();
  }

  virtual void Delete(const Slice& key)
  {

    ReplKey k(key, seq_++, rocksdb::ValueType::kTypeDeletion);
    rocksdb::Slice kSlice = k.Encode();
    rocksdb::Slice value;
    /*
    std::cout << "DELETE cf="  << kDefaultColumnFamilyIdx
      << ":key=" << key.ToString()
      << std::endl;
      */
    // insert a tombstone
    db.rocksdb_->Put(rocksdb::WriteOptions(), kSlice, value);
  }

  virtual void LogData(const Slice& blob)
  {
    rocksdb::VersionEdit edit;
    auto s = edit.DecodeFrom(blob);
    if (s.ok()) {
      //std::cout << "got version edit="
        //<< edit.DebugString() << std::endl;
    }
  }
};

static int walPort = 8192;

int processCursorOpen(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  ReplCursorOpenReq* req = (ReplCursorOpenReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  rocksdb::ColumnFamilyHandle* cf {nullptr};
  do {
    cf = db.openHandle(req->cfid);
    if (cf == nullptr) {
      usleep(1000);
    }
  } while (cf == nullptr); 

  auto iter = db.rocksdb_->NewIterator(rocksdb::ReadOptions(), cf);
  auto local_cursor_id = ++ db.nextCursorId_;
  db.openCursors_.insert(std::make_pair(local_cursor_id, iter));

  ReplCursorOpenResp* resp = nullptr;
  size_t totalSz = sizeof(*resp);
  std::string memKey;
  std::string value;
  SequenceNumber seq;
  std::string inputUserKey;

  if (extraSz) {
    assert(req->seekLast == false);
    assert(req->seekFirst == false);
    inputUserKey = std::string(req->buf, extraSz);
    ReplKey k(inputUserKey, req->seq, ValueType::kTypeValue);
    iter->Seek(k.Encode());
  } else if (req->seekLast == true) {
    iter->SeekToLast();
  } else {
    assert(req->seekFirst == true);
    iter->SeekToFirst();
  }

  if (iter->Valid()) {
    memKey = iter->key().ToString();
    value = iter->value().ToString();
    totalSz += iter->key().size() + iter->value().size();
  } 

  resp = (ReplCursorOpenResp*)malloc(totalSz);
  resp->cursor_id = local_cursor_id;
  resp->status = Status::Code::kOk;

  if (iter->Valid()) {
    resp->kv.putKey(memKey);
    resp->kv.putValue(value);
    resp->is_eof = false;
    std::cout << "OPENCURSOR id=" << resp->cursor_id 
      << " cfid=" << req->cfid
      << " key=" << memKey
      << " value=" << value.substr(0, 10)
      << " seek_key=" << inputUserKey
      << " eof=" << resp->is_eof 
      << std::endl;
  } else {
    resp->is_eof = true;
    std::cout << "OPENCURSOR id=" << resp->cursor_id 
      << " cfid=" << req->cfid
      << " seek_key=" << inputUserKey
      << " eof=" << resp->is_eof << std::endl;
  }

  int err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_CURSOR_OPEN, resp, totalSz, db.lastAppliedSeq_);

  free(resp);

  return err;
}

int processCursorMultiNext(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  rocksdb::ReplCursorMultiNextReq* req = (rocksdb::ReplCursorMultiNextReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  std::string userKey;
  std::string value;
  SequenceNumber seq{0};

  rocksdb::Iterator* iter = nullptr;

  auto mapIter = db.openCursors_.find(req->cursor_id);
  if (mapIter != db.openCursors_.end()) {
    iter = mapIter->second;
  }

  std::vector<std::string> keyArray;
  std::vector<std::string> valueArray;

  if (((req->direction == 1) || (req->direction == -1)) &&
    (iter != nullptr)) {

    for (decltype(req->num_requested) idx = 0; idx < req->num_requested; idx ++) {

      if (req->direction == 1) {
        iter->Next();
      } else {
        assert(req->direction == -1);
        iter->Prev();
      } 
  
      if (iter->Valid()) {
        keyArray.push_back(iter->key().ToString());
        valueArray.push_back(iter->value().ToString());
      } else {
        break;
      }
    } 
  }
  
  rocksdb::ReplCursorMultiNextResp* resp = nullptr;

  size_t totalSz = sizeof(*resp);
  resp = (rocksdb::ReplCursorMultiNextResp*)malloc(totalSz);
  resp->cursor_id = req->cursor_id;
  resp->num_sent = 0;
  resp->keySz = resp->valueSz = 0;

  // iter has buffer which shouldnt get freed before write
  BufferIter bufIter(nullptr, 0);

  if (!iter) {
    resp->is_eof = true;
    resp->status = Status::Code::kNotFound;
    std::cout << "NEXTCURSOR id=" << resp->cursor_id << " not found" << std::endl;
  } else if (keyArray.size()) {
    resp->status = Status::Code::kOk;
    resp->num_sent = keyArray.size();
    resp->is_eof = iter->Valid();

    resp->keySz = bufIter.writeVector(keyArray);
    resp->valueSz = bufIter.writeVector(valueArray);

    assert(resp->keySz + resp->valueSz == bufIter.size());
    totalSz += bufIter.size();
    resp = (rocksdb::ReplCursorMultiNextResp*)realloc(resp, totalSz);
    memcpy(resp->buf, bufIter.data(), bufIter.size());

  } else {
    resp->status = Status::Code::kOk;
    resp->is_eof = true;
  }

  /*
  std::cout << "NEXTCURSOR id=" << resp->cursor_id 
    << " seq=" << seq
    << " direction=" << req->direction
    << " eof=" << resp->is_eof 
    << " key=" << userKey
    << " value=" << value.substr(0, 10)
    << std::endl;
    */

  int err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_CURSOR_MULTI_NEXT, 
    resp, 
    totalSz, 
    db.lastAppliedSeq_);

  free(resp);

  return err;
}

int processCursorNext(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  rocksdb::ReplCursorNextReq* req = (rocksdb::ReplCursorNextReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  rocksdb::ReplCursorNextResp* resp = nullptr;
  size_t totalSz = sizeof(*resp);
  std::string memkey;
  std::string value;
  SequenceNumber seq{0};

  rocksdb::Iterator* iter = nullptr;

  auto mapIter = db.openCursors_.find(req->cursor_id);
  if (mapIter != db.openCursors_.end()) {
    iter = mapIter->second;
  }

  if (iter != nullptr) {
    if (req->direction == 1) {
      iter->Next();
    } else if (req->direction == -1) {
      iter->Prev();
    } else {
      std::cout << "bad direction =" << req->direction << std::endl;
      exit(1);
    }
  
    if (iter->Valid()) {
      memkey = iter->key().ToString();
      value = iter->value().ToString();
      totalSz += memkey.size() + value.size();
    } 
  }
  
  resp = (rocksdb::ReplCursorNextResp*)malloc(totalSz);
  resp->cursor_id = req->cursor_id;
  if (!iter) {
    resp->is_eof = true;
    resp->status = Status::Code::kNotFound;
    std::cout << "NEXTCURSOR id=" << resp->cursor_id << " not found" << std::endl;
  } else if (iter->Valid()) {
    resp->status = Status::Code::kOk;
    resp->kv.putKey(memkey);
    resp->kv.putValue(value);
    resp->is_eof = false;
  } else {
    resp->status = Status::Code::kOk;
    resp->is_eof = true;
  }

  /*
  std::cout << "NEXTCURSOR id=" << resp->cursor_id 
    << " seq=" << seq
    << " direction=" << req->direction
    << " eof=" << resp->is_eof 
    << " key=" << userKey
    << " value=" << value.substr(0, 10)
    << std::endl;
    */

  int err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_CURSOR_NEXT, resp, totalSz, db.lastAppliedSeq_);

  free(resp);

  return err;
}

int processCursorClose(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  rocksdb::ReplCursorCloseReq* req = (rocksdb::ReplCursorCloseReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  rocksdb::ReplCursorCloseResp* resp = nullptr;
  size_t totalSz = sizeof(*resp);
  resp = (rocksdb::ReplCursorCloseResp*)malloc(totalSz);

  auto mapIter = db.openCursors_.find(req->cursor_id);
  if (mapIter != db.openCursors_.end()) {
    delete mapIter->second;
    db.openCursors_.erase(mapIter);
    resp->status = Status::kOk;
  } else {
    resp->status = Status::kNotFound;
  }
  resp->cursor_id = req->cursor_id;

  std::cout << "CLOSECURSOR id=" << resp->cursor_id << std::endl;

  int err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_CURSOR_CLOSE, resp, totalSz, db.lastAppliedSeq_);

  free(resp);

  return err;
}

int processLookup(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  rocksdb::ReplLookupReq* req = (rocksdb::ReplLookupReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  std::string lookupKey(req->key, extraSz);
  uint32_t cfid = req->cfid;

  /*
  std::cout 
    << "read from ReadSocket size=" << extraSz
    << ":cfid=" << cfid
    << ":key=" << lookupKey
    << ":db map size=" << db.handles_.size()
    << ":seq=" << req->seq
    << std::endl;
    */

  ReplLookupResp* resp = nullptr;
  size_t totalSz = sizeof(*resp);

  while (db.lastAppliedSeq_ < req->seq) {
    std::cout << "waiting for wal seq=" << db.lastAppliedSeq_
      << " to reach lookup seq=" << req->seq << std::endl;
    sleep(10);
  }

  rocksdb::ColumnFamilyHandle* cf{nullptr};

  do {
    // TODO hack to remove
    // need to find more reliable way to propagate Schema creation/deletion
    cf = db.openHandle(cfid);
    if (cf == nullptr) {
      sleep(1);
    }
  } while (cf == nullptr);

  int ret = 0;

  ReplKey internalKey(lookupKey, db.lastAppliedSeq_, rocksdb::kValueTypeForSeek);

  if (cf != nullptr)
  {
    std::string value;
    rocksdb::Slice kSlice = internalKey.Encode(); // internal_key ?
    auto s = db.rocksdb_->Get(rocksdb::ReadOptions(), cf, kSlice, &value);

    if (s.ok()) 
    {
      //std::cout << "GET cf=" << cfid 
        //<< " found value=" << value.substr(0, 10) << " key=" << lookupKey 
        //<< std::endl;

      totalSz += value.size();
      resp = (ReplLookupResp*)malloc(totalSz);
      resp->found = true;
      memcpy(resp->value, value.data(), value.size());
      resp->status = Status::Code::kOk;
    }
    else 
    {
      // key not found
      resp = (ReplLookupResp*)malloc(totalSz);
      resp->found = false;
      resp->status = Status::Code::kOk;
      //std::cout << "GET cf=" << cfid 
        //<< " not found key=" << lookupKey 
        //<< std::endl;
    }
  } 
  else 
  {
    resp = (ReplLookupResp*)malloc(totalSz);
    resp->status = Status::Code::kInvalidArgument;
    //std::cout << "GET FATAL not found cf=" << cfid << std::endl;
  }

  int err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_LOOKUP, resp, totalSz, db.lastAppliedSeq_);

  free(resp);

  return err;
}

int processWAL(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  rocksdb::ReplWALUpdate* req = (rocksdb::ReplWALUpdate*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  int ret = 0;

  //std::cout << "Got a WriteBatch"
    //<< " seq=" << req->seq
    //<< ":size=" << extraSz
    //<< ":num updates in batch=" << batch.Count()
    //<< std::endl;

  rocksdb::WriteBatch batch;
  rocksdb::Slice slice(req->buf, extraSz);
  // set contents of batch using Slice
  rocksdb::WriteBatchInternal::SetContents(&batch, slice);


  MapInserter handler(req->seq, db);
  batch.Iterate(&handler);

  // Update db sequence number
  db.lastAppliedSeq_ = req->seq + batch.Count();

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
int processInit(ReplSocket& sock, void* void_req, size_t totalReadSz)
{
  ReplDBReq* req = (ReplDBReq*)void_req;
  size_t extraSz = totalReadSz - sizeof(*req);
  int err = 0;

  std::string remoteIdentity(req->identity, req->identitySize);

  const bool IsNewOffloader = ((db.identity_.size() == 0) && (db.lastAppliedSeq_ == 0));
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
      assert(db.lastAppliedSeq_ == 0);
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
      db.lastAppliedSeq_ = req->seq;
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
      assert (db.lastAppliedSeq_ != 0);
      assert (req->seq != 0);
      if (db.lastAppliedSeq_ <= req->seq) 
      {
        // CASE : old rocksdb re-connecting to old offloader
        responseSeq = db.lastAppliedSeq_;
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

  ReplDBResp* resp = nullptr;
  size_t totalSz = sizeof(*resp) + responseIdentity.size();
  resp = (ReplDBResp*)malloc(totalSz);
  resp->identitySize = responseIdentity.size();
  resp->seq = responseSeq;
  memcpy(resp->identity, responseIdentity.data(), responseIdentity.size());

  err = sock.writeSocket(rocksdb::ReplResponseOp::RESP_INIT1, resp, totalSz, db.lastAppliedSeq_);

  free(resp);

  return err;
}

bool eof = false;

void exit_handler(int signum)
{
  eof = true;
}

std::map<ReplRequestOp, std::function<int(ReplSocket&, void*, size_t)>> opTable =
{
  { rocksdb::ReplRequestOp::OP_INIT1,              processInit },
  { rocksdb::ReplRequestOp::OP_LOOKUP,             processLookup },
  { rocksdb::ReplRequestOp::OP_WAL,                processWAL },
  { rocksdb::ReplRequestOp::OP_CURSOR_OPEN,        processCursorOpen },
  { rocksdb::ReplRequestOp::OP_CURSOR_MULTI_NEXT,  processCursorMultiNext },
  { rocksdb::ReplRequestOp::OP_CURSOR_NEXT,        processCursorNext },
  { rocksdb::ReplRequestOp::OP_CURSOR_CLOSE,       processCursorClose },
};

void serverWorker(int sockfd)
{
  ReplSocket sock(sockfd);

  std::cout << "tid=" << gettid() << " started work on socket=" << sockfd << std::endl;

  while (!eof) 
  {
    void* void_req;
    ReplResponseOp op;
    ssize_t returnSz;

    int err = sock.readSocket(op, &void_req, returnSz);
    if (err < 0) {
      std::cout << "got readheader error" << std::endl;
      eof = true;
      break;
    }

    bool found = false;
    for (auto iter : opTable)
    {
      if (iter.first == op) {

        found = true;

        auto func = iter.second;
        int ret = func(sock, void_req, returnSz);

        free(void_req);

        if (ret != 0) 
        {
          eof = true;
          std::cout << "got error on op=" << op << std::endl;
        }

        break;
      }
    }

    if (!found) {
      std::cout << " UNKNOWN opcode=" << op << std::endl;
      eof = true;
    }
  }

  std::cout << "tid=" << gettid() << " stopped work on socket=" << sockfd << std::endl;
}

int main(int argc, char* argv[])
{
  //kDBPath.append(std::to_string(getpid()));
  //
  struct sigaction action;
  memset(&action, 0, sizeof(struct sigaction));
  action.sa_handler = exit_handler;
  sigaction(SIGINT, &action, NULL);

  bool newInstance = false;
  if (argc > 1) {
    newInstance = atoi(argv[1]);
    if (argc > 2) {
      walPort = atoi(argv[2]);
    }
  }
  db.init(newInstance);


  int listenSocket, newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;
  socklen_t addr_size;

  listenSocket = socket(PF_INET, SOCK_STREAM, 0);

  int enable = 1;
  if (setsockopt(listenSocket, SOL_SOCKET, 
      SO_REUSEADDR, &enable, sizeof(int)) < 0) {
    perror("setsockopt(SO_REUSEADDR) failed");
    exit(1);
  }

  if (setsockopt(listenSocket, SOL_SOCKET, 
      SO_REUSEPORT, &enable, sizeof(int)) < 0) {
    perror("setsockopt(SO_REUSEPORT) failed");
    exit(1);
  }
  
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_port = htons(walPort);
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);  

  auto ret = bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));
  if (ret != 0) {
    perror("bind failed");
    exit(1);
  }

  if(listen(listenSocket,5)==0)
    printf("Listening on %d\n", walPort);
  else {
    perror("listen failed");
    exit(1);
  }

  addr_size = sizeof (serverStorage);

  std::vector<std::future<void>> futVec;

  while (!eof) {

    newSocket = accept(listenSocket, 
      (struct sockaddr *) &serverStorage, &addr_size);
  
    auto fut = std::async(std::launch::async,
      serverWorker, newSocket);

    futVec.emplace_back(std::move(fut));
  }

  for (auto& fut : futVec)
  {
    fut.wait();
  }

  close(listenSocket);

  return 0;
}
