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

/*
 * Use MSG_NOSIGNAL to prevent SIGPIPE on closed tcp connxn
 * if partial write, retry
 */
static ssize_t writeWrap(int fd, const char* buf, size_t count)
{
  ssize_t writeSz = 0;
  do {
    writeSz = ::send(fd, (const void*)buf, count, MSG_NOSIGNAL);
  } while (writeSz < 0 && (errno == EINTR || errno == EAGAIN));
  return writeSz;
}

/*
 * Check interrupted syscall EINTR and EAGAIN, 
 * use WAITALL to get all data in one shot
 * if still get partial reads, retry
 */
static ssize_t readWrap(int fd, char* buf, size_t count)
{
  ssize_t currentReadSz = 0;
  ssize_t partialReadSz = 0;

  while (partialReadSz < (ssize_t)count) {

    do {

      currentReadSz = ::recv(fd,
        buf + partialReadSz, 
        count - partialReadSz, 
        MSG_WAITALL);

    } while ((currentReadSz < 0) && (errno == EINTR || errno == EAGAIN));

    if (currentReadSz < 0) {
      partialReadSz = currentReadSz;
      break;
    } else if (currentReadSz == 0) { 
      // return value 0 means other side closed the socket
      partialReadSz = -1;
      break;
    }

    partialReadSz += currentReadSz;
  }

  return partialReadSz;
}

// =============================================

BufferIter::BufferIter(char* buf, size_t insz)
{
  size_t allocsz = insz;
  if (insz < InitialAllocSize) {
    allocsz = InitialAllocSize;
  } 
  buffer.resize(allocsz);
  if (insz) {
    memcpy(buffer.data(), buf, insz);
  }
}

BufferIter::~BufferIter()
{
}

int BufferIter::readNext(char* outbuf, size_t outsz)
{
  if (offset + outsz <= buffer.size()) {
    memcpy(outbuf, buffer.data() + offset, outsz);
    offset += outsz;
    return 0;
  } else {
    assert("unexpected" == 0);
    return -1;
  }
}

int BufferIter::writeNext(const char* inbuf, size_t insz)
{
  if (offset + insz > buffer.size()) {
    size_t sz = buffer.size();

    do {
      sz = sz << 1;
    } while (offset + insz > sz);

    buffer.resize(sz);
  }
  assert(offset + insz <= buffer.size());
  memcpy(buffer.data() + offset, inbuf, insz);
  offset += insz;
  assert(offset <= buffer.size());
  return 0;
}

int BufferIter::readQueue(size_t numEnt, std::queue<std::string>& stringArray)
{
  std::vector<size_t> sizeArray;
  sizeArray.resize(numEnt);

  readNext((char*)&sizeArray[0], (sizeof(decltype(sizeArray)::value_type) * sizeArray.size()));

  std::vector<char> buf;
  for (auto elem : sizeArray)
  {
    buf.resize(elem);
    bzero(&buf[0], elem);
    readNext(buf.data(), elem);
    stringArray.emplace(buf.data(), elem);
  }

  return 0;
}


/* 
 * @return the size of serialized buffer 
 */
ssize_t BufferIter::writeVector(const std::vector<std::string>& stringArray)
{
  decltype(offset) old_offset = offset;

  std::vector<size_t> sizeArray;

  for (auto& elem : stringArray)
  {
    sizeArray.push_back(elem.size());
  }

  const size_t arraySz = sizeof(decltype(sizeArray)::value_type) * sizeArray.size();

  // first we write out an array containing sizes of each string
  writeNext((char*)sizeArray.data(), arraySz);

  for (auto& elem : stringArray)
  {
    writeNext(elem.c_str(), elem.size());
  }

  assert(offset - old_offset > arraySz);
  return (offset - old_offset);
}
// =============================================

ReplSocket::ReplSocket()
{
}

ReplSocket::ReplSocket(int sockfd)
{
  sock_fd = sockfd;
}

ReplSocket::~ReplSocket()
{
  this->close();
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
    this->close();
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

    ssize_t writeSz = writeWrap(sock_fd, (const char*)&header, sizeof(header));

    if (writeSz != sizeof(header)) {
      err = -errno;
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "write failed sz=%ld expected=%ld errno=%d", 
        writeSz, 
        sizeof(header),
        errno);
      break;
    }

    writeSz = writeWrap(sock_fd, (const char*)data, totalSz);

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

    ssize_t readSz = readWrap(sock_fd, (char*)&respHeader, sizeof(respHeader));

    if (readSz != sizeof(respHeader)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl header read failed errno=", errno);
      err = -errno;
      break;
    }

    if (seqPtr) {
      *seqPtr = respHeader.seq;
    }

    char* lresp = (char*)malloc(respHeader.size);
    readSz = readWrap(sock_fd, (char*)lresp, respHeader.size);

    if (readSz != (ssize_t)respHeader.size) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl data read failed errno=", errno);
      err = -errno;
      break;
    }

    op = respHeader.op;
    returnSz = readSz;
    *returnedData = lresp;

  } while (0);

  if (err < 0) {
    this->close();
  }

  return err;
}

bool ReplSocket::IsOpen() const
{
  return (sock_fd != -1);
}

int ReplSocket::close()
{
  int err = 0;
  if (sock_fd != -1) {
    err = ::close(sock_fd);
    if (err == 0) {
      sock_fd = -1;
    }
  }
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
    std::unique_ptr<ReplDBReq, free_delete> initReq ((ReplDBReq*)malloc (totalSz));
    initReq->seq = lastSequence;
    initReq->identitySize = guid.size();
    memcpy(initReq->identity, guid.data(), guid.size());

    err = writeSock.writeSocket(OP_INIT1, (void*)initReq.get(), totalSz, 0);
    if (err < 0) {
      break;
    }

    ReplDBResp* lresp;
    ssize_t readSz = 0;
    ReplResponseOp op = OP_WILDCARD;
    err = writeSock.readSocket(op, (void**)&lresp, readSz);
    if (op != RESP_INIT1 || err < 0) {
      assert(err < 0);
      break;
    }

    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "REPL Initialized for rocksdb seq=%llu server seq=%llu",
      lastSequence, lresp->seq);

    lastReplSequence = lresp->seq;
    // TODO ; process guid
    free(lresp);

  } while (0);

  if (err < 0) {
    writeSock.close();
    readSock.close();
  }

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
  
        std::unique_ptr<ReplWALUpdate, free_delete> sw ((ReplWALUpdate*)malloc (totalSz));
        memcpy(sw->buf, batch.data(), batch.size());
        sw->seq = res.sequence;

        int err = writeSock.writeSocket(OP_WAL, (void*)sw.get(), totalSz, 0);
        (void)err;
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
  Status s;
  if (writeSock.IsOpen()) {
    Slice slice = WriteBatchInternal::Contents(&newBatch);
    replLogList.push_back(slice.ToString());
  } else {
    s = Status::IOError("connxn closed");
  }
  return s;
}

Status ReplThreadInfo::FlushReplLog()
{
  auto& logger = info_log;
  (void)logger;

  Status status;

  if (!writeSock.IsOpen()) {
    return Status::IOError("connxc closed");
  }

  for (auto& elem : replLogList)
  {
    WriteBatch batch(elem);
    Slice slice = WriteBatchInternal::Contents(&batch);
    auto seq = WriteBatchInternal::Sequence(&batch);

    ssize_t totalSz = sizeof(ReplWALUpdate) + batch.GetDataSize();

    // TODO : combine into an operator new + ctor
    std::unique_ptr<ReplWALUpdate, free_delete> sw ((ReplWALUpdate*)malloc (totalSz));
    memcpy(sw->buf, slice.data(), slice.size());
    sw->seq = seq;

    int err = writeSock.writeSocket(OP_WAL, (void*)sw.get(), totalSz, 0);

    if (err < 0) {
      status = Status::IOError("connxn broken");
      writeSock.close();
      break;
    } else {
      lastReplSequence = seq + batch.Count() - 1; 
    }

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

  return status;
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
    std::unique_ptr<ReplLookupReq, free_delete> lreq ((ReplLookupReq*)malloc (totalSz));
    lreq->cfid = column_family->GetID();
    memcpy(lreq->key, key.data(), key.size());
    lreq->seq = seq;

    int err = readSock.writeSocket(OP_LOOKUP, (void*)lreq.get(), totalSz, 0);
    (void)err;

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
    std::unique_ptr<ReplCursorCloseReq> oc (new ReplCursorCloseReq);
    const ssize_t totalSz = sizeof(ReplCursorCloseReq);
    oc->cursor_id = remote_cursor_id_;

    ReplCursorCloseResp* resp{nullptr};

    do {

      err = t->readSock.writeSocket(OP_CURSOR_CLOSE, (void*)oc.get(), totalSz, 0);
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

        ReplKey replKey(key_);
        auto userKey = replKey.userKey();
        auto seq = replKey.seq();
        auto val = replKey.val();
        ParsedInternalKey pkey(userKey, seq, val);
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

    std::unique_ptr<ReplCursorOpenReq, free_delete> oc ((ReplCursorOpenReq*)malloc (sizeof(ReplCursorOpenReq) + k.size()));
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

    SeekInternal(oc.get(), newSlice.size());
  }
  virtual void SeekToFirst() override 
  {
    std::unique_ptr<ReplCursorOpenReq> oc (new ReplCursorOpenReq);
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = true;
    oc->seekLast = false;
    oc->numKeysPerNext = 1;

    SeekInternal(oc.get(), 0);
  }
  virtual void SeekToLast() override 
  {
    std::unique_ptr<ReplCursorOpenReq> oc (new ReplCursorOpenReq);
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = false;
    oc->seekLast = true;
    oc->numKeysPerNext = 1;

    SeekInternal(oc.get(), 0);

  }

  void MultiNextInternal(int direction) 
  {
    ReplThreadInfo* t = &repl_thread_info_;

    int err = 0;
    std::unique_ptr<ReplCursorMultiNextReq> oc (new ReplCursorMultiNextReq);
    const ssize_t totalSz = sizeof(ReplCursorMultiNextReq);
    oc->cursor_id = remote_cursor_id_;
    oc->direction = direction;
    oc->num_requested = 100;

    ReplCursorMultiNextResp* resp{nullptr};

    assert(is_remote_eof_ == false);

    do {

      err = t->readSock.writeSocket(OP_CURSOR_MULTI_NEXT, (void*)oc.get(), totalSz, 0);
      if (err < 0) {
        break;
      }

      ssize_t readSz;
      ReplResponseOp op;

      err = t->readSock.readSocket(op, (void**)&resp, readSz, &t->lastAckedSequence);

      if (op != RESP_CURSOR_MULTI_NEXT || err < 0) {
        valid_ = false;
        break;
      }

      // TODO check resp->cursor_id matches with request

      assert(readSz == (ssize_t)(sizeof(*resp) + resp->keySz + resp->valueSz));

      if (resp->status == Status::Code::kOk) {

        valid_ = true;

        if (resp->is_eof) {
          is_remote_eof_ = true;
        }

        if (resp->num_sent) {

          BufferIter keyIter(resp->buf, resp->keySz);
          BufferIter valueIter(resp->buf + resp->keySz, resp->valueSz);
  
          keyIter.readQueue(resp->num_sent, cachedKeys_);
          valueIter.readQueue(resp->num_sent, cachedValues_);
  
          assert(cachedKeys_.size() == resp->num_sent);
          assert(cachedValues_.size() == resp->num_sent);
        }

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
  }

  void NextInternal(int direction) 
  {
    ReplThreadInfo* t = &repl_thread_info_;

    int err = 0;
    std::unique_ptr<ReplCursorNextReq> oc (new ReplCursorNextReq);
    const ssize_t totalSz = sizeof(ReplCursorNextReq);
    oc->cursor_id = remote_cursor_id_;
    oc->direction = direction;

    ReplCursorNextResp* resp{nullptr};

    do {

      err = t->readSock.writeSocket(OP_CURSOR_NEXT, (void*)oc.get(), totalSz, 0);
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

        ReplKey replKey(key_);
        auto userKey = replKey.userKey();
        auto seq = replKey.seq();
        auto val = replKey.val();
        ParsedInternalKey pkey(userKey, seq, val);
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
  }

  // if internal key cache empty
  //   get more from offloader
  void CachedNext(int direction) 
  {
    if (!cachedKeys_.size()) {
      if (is_remote_eof_ == true) {
        valid_ = false;
      } else {
        MultiNextInternal(direction);
      }
    }

    if (cachedKeys_.size()) {
      key_ = cachedKeys_.front();
      value_ = cachedValues_.front();
  
      ReplKey replKey(key_);
      auto userKey = replKey.userKey();
      auto seq = replKey.seq();
      auto val = replKey.val();
      ParsedInternalKey pkey(userKey, seq, val);
      internalKey_.SetFrom(pkey);
  
      cachedKeys_.pop();
      cachedValues_.pop();
    }
  }

  virtual void Next() override
  {
    CachedNext(1);
  }
  virtual void Prev() override 
  {
    CachedNext(-1);
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
    // TODO understand usage of this call
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

  // original seqnum on which seek was done
  SequenceNumber seqnum_; 

  int32_t remote_cursor_id_ = -1; // TODO define invalid id

  // internal cache of keys
  std::queue<std::string> cachedKeys_;
  std::queue<std::string> cachedValues_;
  
  // use this flag to invalidate local iterator
  // after last cache sent by offloader
  // has been exhausted
  // this flag is set and used only in MultiNext
  bool is_remote_eof_ { false }; 

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
