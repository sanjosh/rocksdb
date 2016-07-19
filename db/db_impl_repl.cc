#include "db/db_impl.h"
#include "db/db_repl.h"
#include "db/auto_roll_logger.h"
#include "db/write_batch_internal.h" 
#include "rocksdb/env.h" // Env
#include "rocksdb/status.h" // Status
#include "table/merger.h" // MergeIteratorBuilder

#include <stdio.h> // malloc
#include <unistd.h> // write
#include <sys/socket.h>
#include <netinet/in.h> 
#include <arpa/inet.h> //  inet_addr

namespace rocksdb {

ReplSocket::ReplSocket()
{
}

ReplSocket::ReplSocket(int sockfd)
{
  sock_fd = sockfd;
}

ReplSocket::~ReplSocket()
{
  close(sock_fd);
}

int ReplSocket::connect(const std::string& in_addr, int in_port,
    std::shared_ptr<rocksdb::Logger> in_logger)
{
  this->addr = in_addr;
  this->port = in_port;
  this->logger = in_logger;

  sock_fd = socket(PF_INET, SOCK_STREAM, 0);

  struct sockaddr_in server_addr;
  socklen_t server_addr_size;

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(addr.c_str());
  memset(server_addr.sin_zero, '\0', sizeof(server_addr.sin_zero));
  server_addr_size = sizeof(server_addr);

  int err = ::connect(sock_fd, (struct sockaddr*)&server_addr, server_addr_size);
  if (err < 0) 
  {
    close(sock_fd);
    sock_fd = -1;
    return errno;
  }
  return err;
}

int ReplSocket::writeSocket(ReplRequestOp op, const void* data, const size_t totalSz, SequenceNumber seq)
{
  int err = 0;

  do {

    std::unique_lock<std::mutex> l(sock_mutex);

    ReplRequestHeader header;
    header.op = op;
    header.size = totalSz;
    header.seq = seq;

    ssize_t writeSz = ::write(sock_fd, (const void*)&header, sizeof(header));

    if (writeSz != sizeof(header)) {
      err = -errno;
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "write failed sz=%ld expected=%ld errno=%d", 
        writeSz, 
        sizeof(header),
        errno);
      break;
    }

    writeSz = write(sock_fd, (const void*)data, totalSz);

    if (writeSz != (ssize_t)totalSz) {
      err = -errno;
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "write failed sz=%ld expected=%ld errno=%d", 
        writeSz, 
        totalSz,
        errno);
      break;
    }

  } while (0);

  return err;
}

int ReplSocket::readSocket(ReplResponseOp& op, void** returnedData, ssize_t &returnSz, SequenceNumber* seqPtr)
{
  int err = 0;
  op = OP_WILDCARD;

  do
  {
    std::unique_lock<std::mutex> l(sock_mutex);

    // read resp
    ReplResponseHeader respHeader;
    ssize_t readSz;

    do {
      readSz = read(sock_fd, (void*)&respHeader, sizeof(respHeader));
    } while ((readSz < 0) && (errno == EINTR || errno == EAGAIN));

    if (readSz != sizeof(respHeader)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl header read failed");
      err = -errno;
      break;
    }

    if (seqPtr) {
      *seqPtr = respHeader.seq;
    }

    char* lresp = (char*)malloc(respHeader.size);
    do {
      readSz = read(sock_fd, (void*)lresp, respHeader.size);
    } while ((readSz < 0) && (errno == EINTR || errno == EAGAIN));

    if (readSz != (ssize_t)respHeader.size) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl data read failed");
      err = -errno;
      break;
    }

    op = respHeader.op;
    returnSz = readSz;
    *returnedData = lresp;

  } while (0);

  return err;
}

// =====================

int ReplThreadInfo::initialize(const std::string& guid,
    SequenceNumber lastSequence,
    const std::string& addr,
    int port)
{
  auto& logger = info_log;

  int err = 0;
  do {

    err = writeSock.connect(addr, port, logger);

    if (err < 0)
    {
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "sockets could not start to %s:%d error=%d",
        addr.c_str(), 
        port,
        err);
      break;
    }

    err = readSock.connect(addr, port, logger);

    if (err < 0)
    {
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "sockets could not start to %s:%d error=%d",
        addr.c_str(), 
        port,
        err);
      break;
    }

    // DO INITIALIZATION HANDSHAKE
    ssize_t totalSz = sizeof(ReplDBReq) + guid.size();
    ReplDBReq* initReq = (ReplDBReq*)malloc(totalSz);
    initReq->seq = lastSequence;
    initReq->identitySize = guid.size();
    memcpy(initReq->identity, guid.data(), guid.size());

    err = writeSock.writeSocket(OP_INIT1, initReq, totalSz, 0);
    if (err < 0) {
      break;
    }

    ReplDBResp* lresp;
    ssize_t readSz;
    ReplResponseOp op;
    err = writeSock.readSocket(op, (void**)&lresp, readSz);
    if (op != OP_INIT1 || err < 0) {
      break;
    }

    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "REPL Initialized for rocksdb seq=%llu server seq=%llu",
      lastSequence, lresp->seq);

    lastReplSequence = lresp->seq;
    // TODO ; process guid
    free(lresp);

  } while (0);

  return err;
}

void ReplThreadInfo::walUpdater()
{
  auto& logger = info_log;

  started.store(true, std::memory_order_release);
  Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread started");

  std::unique_ptr<TransactionLogIterator> iter;


  while (!stop.load(std::memory_order_acquire)) {
      
    iter.reset();
    /*
    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "Repl thread asking for logs from seq=%llu",
      lastReplSequence + 1);
      */
        
    while (!db->GetUpdatesSince(lastReplSequence + 1, &iter).ok()) {
      if (!stop.load(std::memory_order_acquire)) {
        break;
      }
      sleep(1);
    }
      
    if (iter.get() == nullptr) {
      Env::Default()->SleepForMicroseconds(1000); // TODO cond var
      continue;
    }

    for (; iter->Valid(); iter->Next()) {

      BatchResult res = iter->GetBatch();

      auto batch = res.writeBatchPtr->Data();

      // When you commit a WriteBatch at seq=M with (say) 3 upd
      // rocksdb increments sequence number by 3 to M+3
      // i.e. intermediate numbers are skipped
      // Now, if you ask for a WriteBatch starting from seq M+1
      // GetUpdatesSince() will return the update with seq=M even 
      // though it is less than what you asked, because it is 
      // giving back the update that actually has seq=M

      {
        ssize_t totalSz = sizeof(ReplWALUpdate) + batch.size();
  
        // TODO : combine into an operator new + ctor
        ReplWALUpdate* sw = (ReplWALUpdate*) malloc(totalSz);
        memcpy(sw->buf, batch.data(), batch.size());
        sw->seq = res.sequence;

        int err = writeSock.writeSocket(OP_WAL, sw, totalSz, 0);
        (void)err;

        free(sw);
      }

      lastReplSequence = res.sequence + res.writeBatchPtr->Count() - 1;

      Log(InfoLogLevel::INFO_LEVEL, logger, 
        "Repl thread sent seq=%llu actual batch=%lu numUpd=%d ", 
        res.sequence,
        batch.size(),
        res.writeBatchPtr->Count()
      );
    }
  }
  has_stopped.store(true, std::memory_order_release);
  Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread exiting");
}

Status ReplThreadInfo::AddToReplLog(WriteBatch& newBatch)
{
  Slice s = WriteBatchInternal::Contents(&newBatch);
  replLogList.push_back(s.ToString());
  return Status::OK();
}

Status ReplThreadInfo::FlushReplLog()
{
  auto& logger = info_log;
  (void)logger;

  for (auto& elem : replLogList)
  {
    WriteBatch batch(elem);
    Slice s = WriteBatchInternal::Contents(&batch);
    auto seq = WriteBatchInternal::Sequence(&batch);

    ssize_t totalSz = sizeof(ReplWALUpdate) + batch.GetDataSize();

    // TODO : combine into an operator new + ctor
    ReplWALUpdate* sw = (ReplWALUpdate*) malloc(totalSz);
    memcpy(sw->buf, s.data(), s.size());
    sw->seq = seq;

    int err = writeSock.writeSocket(OP_WAL, sw, totalSz, 0);
    (void)err;

    free(sw);

    lastReplSequence = seq + batch.Count() - 1; 

    /*
    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "Repl thread sent seq=%llu actual batch=%lu numUpd=%d ", 
      lastReplSequence,
      elem.size(),
      batch.Count()
      );
      */
  }

  replLogList.clear();

  return Status::OK();
}

void DBImpl::ReplThreadBody(void* arg)
{
  ReplThreadInfo* t = reinterpret_cast<ReplThreadInfo*>(arg);
  t->walUpdater();
}


Status ReplThreadInfo::Get(const ReadOptions& options, 
  ColumnFamilyHandle* column_family,
  const Slice& key, 
  SequenceNumber seq,
  std::string* value,
  bool* value_found)
{
  Status status;

  do {

    const ssize_t totalSz = sizeof(ReplLookupReq) + key.size();
    ReplLookupReq* lreq = (ReplLookupReq*) malloc(totalSz);
    lreq->cfid = column_family->GetID();
    memcpy(lreq->key, key.data(), key.size());
    lreq->seq = seq;

    int err = readSock.writeSocket(OP_LOOKUP, lreq, totalSz, 0);
    (void)err;

    free((void*)lreq);

    ReplLookupResp* lresp;
    ssize_t readSz;
    ReplResponseOp op;
    err = readSock.readSocket(op, (void**)&lresp, readSz, &lastAckedSequence);

    if (op != RESP_LOOKUP || err < 0) {
      break;
    }

    if (lresp->found) {
      if (value_found) {
        *value_found = lresp->found; 
      }
      if (value) {
        value->assign(lresp->value, readSz - sizeof(ReplLookupResp));
      }
      status = Status::OK();
    } else {
      status = Status::NotFound();
    }

    free(lresp);

  } while (0);
  
  return status;
}

class ReplIterator : public InternalIterator {
public:
  ReplIterator(ReplThreadInfo& repl_thread_info, 
    uint32_t cfid,
    std::shared_ptr<rocksdb::Logger> info_log,
    SequenceNumber seqnum,
    const ReadOptions& read_options)
    : repl_thread_info_(repl_thread_info)
    , cfid_(cfid)
    , logger(info_log)
    , seqnum_(seqnum)
    , read_options_(read_options)
  {
  }

  ~ReplIterator() 
  {
    CloseCursor();
  }

  void CloseCursor()
  {

    ReplThreadInfo* t = &repl_thread_info_;

    int err = 0;
    ReplCursorCloseReq* oc = (ReplCursorCloseReq*)malloc(sizeof(ReplCursorCloseReq));
    const ssize_t totalSz = sizeof(ReplCursorCloseReq);
    oc->cursor_id = remote_cursor_id_;

    ReplCursorCloseResp* resp{nullptr};

    do {

      err = t->readSock.writeSocket(OP_CURSOR_CLOSE, oc, totalSz, 0);
      if (err < 0) {
        break;
      }

      ssize_t readSz;
      ReplResponseOp op;

      err = t->readSock.readSocket(op, (void**)&resp, readSz, &t->lastAckedSequence);

      if (op != RESP_CURSOR_CLOSE || err < 0) {
        break;
      }

    } while (0);
    
    free(resp);
    free(oc);

    remote_cursor_id_ = -1;
    valid_ = false;
  }

  virtual bool Valid() const override 
  {
    return valid_;
  }

  virtual int SeekInternal(ReplCursorOpenReq* oc, size_t extraSz) 
  {
    ReplThreadInfo* t = &repl_thread_info_;

    /**
     * Seek() can be called on same iter more than once 
     * through the mongo-rocks layer
     * if first seek fails, the second is seekToLast
     * Handle that case here by closing and reinitializing cursor
     */
    if (remote_cursor_id_ != -1) {
      CloseCursor();
    }

    int err = 0;
    ReplCursorOpenResp* resp{nullptr};

    do {

      const ssize_t totalSz = sizeof(ReplCursorOpenReq) + extraSz;

      err = t->readSock.writeSocket(OP_CURSOR_OPEN, oc, totalSz, 0);
      if (err < 0) {
        Log(InfoLogLevel::ERROR_LEVEL, logger, 
          "cursor write got error err=%d", err);
        break;
      }

      ssize_t readSz;
      ReplResponseOp op;

      err = t->readSock.readSocket(op, (void**)&resp, readSz, &t->lastAckedSequence);
      if (op != RESP_CURSOR_OPEN || err < 0) {
        Log(InfoLogLevel::ERROR_LEVEL, logger, 
          "cursor read got error err=%d", err);
        break;
      }

      remote_cursor_id_ = resp->cursor_id;

      if ((resp->status == Status::Code::kOk) && 
          (!resp->is_eof)) {
        valid_ = true;
        key_ = resp->kv.getKey();
        value_ = resp->kv.getValue();
        ParsedInternalKey pkey(key_, resp->seq, kTypeValue);
        internalKey_.SetFrom(pkey);
      } else {
        valid_ = false;
      }

      /*
      Log(InfoLogLevel::INFO_LEVEL, logger, 
          "cursor seek got cfid=%d valid=%d key=%s value=%s seq=%llu", 
          cfid_, valid_, key_.c_str(), value_.c_str(), resp->seq);
          */

    } while (0);

    free(resp);
    return err;
  }

  virtual void Seek(const Slice& k) override 
  {
    if (k.size() == sizeof(SequenceNumber))  {
      // if incoming key doesnt have user key, its a seek first
      // TODO need to send seqnum to offloader
      return SeekToFirst();
    }

    ReplCursorOpenReq* oc = (ReplCursorOpenReq*)malloc(sizeof(ReplCursorOpenReq) + k.size());
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = false;
    oc->seekLast = false;
    oc->numKeysPerNext = 1;

    // input slice contains key + sequenceNumber
    // See call to SetInternalKey(target, seq) in DBIter::Seek()
    // extract userKey from this
    Slice newSlice(k.data(), k.size() - sizeof(SequenceNumber));

    memcpy(oc->buf, newSlice.data(), newSlice.size());

    SeekInternal(oc, newSlice.size());

    free(oc);
  }
  virtual void SeekToFirst() override 
  {
    ReplCursorOpenReq* oc = (ReplCursorOpenReq*)malloc(sizeof(ReplCursorOpenReq));
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = true;
    oc->seekLast = false;
    oc->numKeysPerNext = 1;

    SeekInternal(oc, 0);

    free(oc);
  }
  virtual void SeekToLast() override 
  {
    ReplCursorOpenReq* oc = (ReplCursorOpenReq*)malloc(sizeof(ReplCursorOpenReq));
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = false;
    oc->seekLast = true;
    oc->numKeysPerNext = 1;

    SeekInternal(oc, 0);

    free(oc);
  }


  void NextInternal(int direction) 
  {
    ReplThreadInfo* t = &repl_thread_info_;

    int err = 0;
    ReplCursorNextReq* oc = (ReplCursorNextReq*)malloc(sizeof(ReplCursorNextReq));
    const ssize_t totalSz = sizeof(ReplCursorNextReq);
    oc->cursor_id = remote_cursor_id_;
    oc->direction = direction;

    ReplCursorNextResp* resp{nullptr};

    do {

      err = t->readSock.writeSocket(OP_CURSOR_NEXT, oc, totalSz, 0);
      if (err < 0) {
        break;
      }

      ssize_t readSz;
      ReplResponseOp op;

      err = t->readSock.readSocket(op, (void**)&resp, readSz, &t->lastAckedSequence);

      if (op != RESP_CURSOR_NEXT || err < 0) {
        valid_ = false;
        break;
      }

      if ((resp->status == Status::Code::kOk) && 
          (!resp->is_eof)) {
        valid_ = true;
        key_ = resp->kv.getKey();
        value_ = resp->kv.getValue();
        ParsedInternalKey pkey(key_, resp->seq, kTypeValue);
        internalKey_.SetFrom(pkey);
      } else {
        valid_ = false;
      }

      /*
      Log(InfoLogLevel::INFO_LEVEL, logger, 
          "cursor next got cfid=%d valid=%d key=%s value=%s seq=%llu", 
          cfid_, valid_, key_.c_str(), value_.c_str(), resp->seq);
          */

    } while (0);
    
    free(resp);
    free(oc);
  }

  virtual void Next() override
  {
    NextInternal(1);
  }
  virtual void Prev() override 
  {
    NextInternal(-1);
  }
  virtual Slice key() const override
  {
    return internalKey_.Encode();
  }
  virtual Slice value() const override
  {
    return value_;
  }
  virtual Status status() const override
  {
    return remoteStatus_;
  }
  virtual Status PinData() override
  {
    return Status::OK();
  }
  virtual Status ReleasePinnedData() override
  {
    return Status::OK();
  }
  virtual bool IsKeyPinned() const override
  {
    // if true, then DBIter::saved_key_ doesn't make a copy
    return false;
  }

  private:

  ReplThreadInfo& repl_thread_info_;

  uint32_t cfid_;
  std::shared_ptr<rocksdb::Logger> logger = nullptr;
  SequenceNumber seqnum_;

  int32_t remote_cursor_id_ = -1; // TODO define invalid id

  InternalKey internalKey_; 
  std::string key_;
  std::string value_;
  Status remoteStatus_;

  ReadOptions read_options_;

  bool valid_ = false;

  ReplIterator(const ReplIterator&) = delete;
  void operator =(const ReplIterator&) = delete;

};

void ReplThreadInfo::AddIterators(uint32_t cfid,
  const ReadOptions& read_options,
  const EnvOptions& soptions,
  MergeIteratorBuilder* merge_iter_builder) {

  SequenceNumber seqnum = 0; // TODO retrieve based on cur seq

  if (read_options.snapshot) {
    seqnum = read_options.snapshot->GetSequenceNumber();
  }

  auto* arena = merge_iter_builder->GetArena();

  auto mem = arena->AllocateAligned(sizeof(ReplIterator));
  merge_iter_builder->AddIterator(
    new (mem) ReplIterator(*this, cfid, info_log, seqnum, read_options));
}

}
