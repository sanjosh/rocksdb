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

static int ConnectSocket(std::string& addr, int port, int& sock_fd)
{
  sock_fd = socket(PF_INET, SOCK_STREAM, 0);

  struct sockaddr_in server_addr;
  socklen_t server_addr_size;

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(addr.c_str());
  memset(server_addr.sin_zero, '\0', sizeof(server_addr.sin_zero));
  server_addr_size = sizeof(server_addr);

  int err = connect(sock_fd, (struct sockaddr*)&server_addr, server_addr_size);
  if (err < 0) 
  {
    close(sock_fd);
    sock_fd = -1;
    return errno;
  }
  return err;
}

class ReplSocket
{
  int writeSocket(int sockfd, ReplRequestOp op, 
    const void* data, const size_t totalSz)
  {

    int err = 0;

    do {

      ReplRequestHeader header;
      header.op = op;
      header.size = totalSz;

      ssize_t writeSz = 0;

      writeSz = write(socket, (const void*)&header, sizeof(header));

      if (writeSz != sizeof(header)) {
        err = -errno;
        Log(InfoLogLevel::ERROR_LEVEL, logger, 
          "write failed sz=%ld expected=%ld errno=%d", 
          writeSz, 
          sizeof(header),
          errno);
        break;
      }

      writeSz = write(socket, (const void*)data, totalSz);

      if (writeSz != totalSz) {
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

  int readSocket(int sockfd, ReplResponseOp op, 
    const void* data, const size_t size)
  {
  }
};

int ReplThreadInfo::initialize(const std::string& guid,
    SequenceNumber lastSequence)
{
  auto& logger = info_log;

  int err = 0;
  do {

    err = ConnectSocket(addr, port, socket);

    if (err < 0)
    {
      Log(InfoLogLevel::INFO_LEVEL, logger, 
        "sockets could not start to %s:%d error=%d",
        addr.c_str(), 
        port,
        err);
      break;
    }

    ssize_t totalSz = sizeof(ReplDatabaseInit) + guid.size();

    ReplRequestHeader header;
    header.op = OP_INIT1;
    header.size = totalSz;

    ssize_t writeSz = write(socket, (const void*)&header, sizeof(header));

    if (writeSz != sizeof(header)) {
      err = -errno;
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "write failed sz=%ld expected=%ld errno=%d", 
        writeSz, 
        sizeof(header),
        errno);
      break;
    }

    ReplDatabaseInit* initReq = (ReplDatabaseInit*)malloc(totalSz);

    initReq->seq = lastSequence;
    initReq->identitySize = guid.size();
    memcpy(initReq->identity, guid.data(), guid.size());

    writeSz = write(socket, (const void*)initReq, totalSz);

    if (writeSz != totalSz) {
      err = -errno;
      Log(InfoLogLevel::ERROR_LEVEL, logger, 
        "write failed sz=%ld expected=%ld errno=%d", 
        writeSz, 
        totalSz,
        errno);
      break;
    }

    // read resp
    ReplResponseHeader respHeader;
    ssize_t readSz = read(socket, (void*)&respHeader, sizeof(respHeader));

    if (readSz != sizeof(respHeader)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl header read failed");
      err = -errno;
      break;
    }

    ReplDatabaseResp* lresp = (ReplDatabaseResp*) malloc(respHeader.size);
    readSz = read(socket, (void*)lresp, respHeader.size);

    if (readSz != (ssize_t)respHeader.size) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl data read failed");
      err = -errno;
      break;
    }

    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "rocksdb seq=%llu server seq=%llu",
      lastSequence, lresp->seq);

    lastReplSequence = lresp->seq;

  } while (0);

  return err;
}

void ReplThreadInfo::walUpdater()
{
  auto& logger = info_log;

  started.store(true, std::memory_order_release);
  Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread started");

  std::unique_ptr<TransactionLogIterator> iter;

  ReplRequestHeader header;
  header.op = OP_WAL;

  while (!stop.load(std::memory_order_acquire)) {
      
    iter.reset();
    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "Repl thread asking for logs from seq=%llu",
      lastReplSequence + 1);
        
    while (!db->GetUpdatesSince(lastReplSequence + 1, &iter).ok()) {
      if (!stop.load(std::memory_order_acquire)) {
        break;
      }
    }
      
    if (iter.get() == nullptr) {
      Env::Default()->SleepForMicroseconds(100); // TODO cond var
      continue;
    }

    for (; iter->Valid(); iter->Next()) {

      BatchResult res = iter->GetBatch();

      auto batch = res.writeBatchPtr->Data();
      ssize_t totalSz = sizeof(ReplWALUpdate) + batch.size();

      // TODO : combine into an operator new + ctor
      ReplWALUpdate* sw = (ReplWALUpdate*) malloc(totalSz);
      memcpy(sw->buf, batch.data(), batch.size());
      sw->seq = res.sequence;

      // When you commit a WriteBatch at seq=M with (say) 3 upd
      // rocksdb increments sequence number by 3 to M+3
      // i.e. intermediate numbers are skipped
      // Now, if you ask for a WriteBatch starting from seq M+1
      // GetUpdatesSince() will return the update with seq=M even 
      // though it is less than what you asked, because it is 
      // giving back the update that actually has seq=M
      lastReplSequence = res.sequence + res.writeBatchPtr->Count() - 1;

      {
        std::unique_lock<std::mutex> l(sock_mutex);

        header.size = totalSz;

        ssize_t writeSz = write(socket, (const void*)&header, sizeof(header));

        if (writeSz != sizeof(header)) {
          Log(InfoLogLevel::ERROR_LEVEL, logger, 
            "write failed sz=%ld expected=%ld errno=%d", 
            writeSz, 
            sizeof(header),
            errno);
          break;
        }

        writeSz = write(socket, (const void*)sw, totalSz);

        if (writeSz != totalSz) {
          Log(InfoLogLevel::ERROR_LEVEL, logger, 
            "write failed sz=%ld expected=%ld errno=%d", 
            writeSz, 
            totalSz,
            errno);
        }
      }

      free(sw);

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

  ReplThreadInfo* t = this;
  auto& logger = info_log;

  ReplRequestHeader header;
  header.op = OP_LOOKUP;

  ReplResponseHeader respHeader;

  do {

    std::unique_lock<std::mutex> l(sock_mutex);

    const ssize_t totalSz = sizeof(ReplLookupRequest) + key.size();

    header.size = totalSz;
    ssize_t writeSz = write(t->socket, (const void*)&header, sizeof(header));
    if (writeSz != sizeof(header)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl header write failed");
      status = Status::IOError("Repl header write failed");
      break;
    }

    ReplLookupRequest* lreq = (ReplLookupRequest*) malloc(totalSz);
    lreq->cfid = column_family->GetID();
    memcpy(lreq->key, key.data(), key.size());
    lreq->seq = seq;

    writeSz = write(t->socket, (const void*)lreq, totalSz);
    free((void*)lreq);

    if (writeSz != totalSz) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl data write failed");
      status = Status::IOError("Repl data write failed");
      break;
    }

    ssize_t readSz = read(t->socket, (void*)&respHeader, sizeof(respHeader));

    if (readSz != sizeof(respHeader)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl header read failed");
      status = Status::IOError("Repl header read failed");
      break;
    }

    ReplLookupResponse* lresp = (ReplLookupResponse*) malloc(respHeader.size);
    readSz = read(t->socket, (void*)lresp, respHeader.size);

    if (readSz != (ssize_t)respHeader.size) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl data read failed");
      status = Status::IOError("Repl data read failed");
      break;
    }

    if (lresp->found) {
      if (value_found) {
        *value_found = lresp->found; 
      }
      if (value) {
        value->assign(lresp->value, respHeader.size);
      }
      status = Status::OK();
    } else {
      status = Status::NotFound();
    }
  } while (0);
  
  return status;
}

class ReplIterator : public InternalIterator {
public:
  ReplIterator(ReplThreadInfo& repl_thread_info, 
    uint32_t cfid,
    SequenceNumber seqnum,
    const ReadOptions& read_options)
    : repl_thread_info_(repl_thread_info)
    , cfid_(cfid)
    , seqnum_(seqnum)
    , read_options_(read_options)
  {
  }

  ~ReplIterator() 
  {
    //ReplCursorClose op;
    //op.cursor_id = 0;
  }

  virtual bool Valid() const override 
  {
    return valid_;
  }

  virtual void SeekInternal(ReplCursorOpenReq* oc) 
  {
    ReplThreadInfo* t = &repl_thread_info_;

    const ssize_t totalSz = sizeof(ReplCursorOpenReq) + oc->size;
    ReplRequestHeader header;
    header.op = ReplRequestOp::OP_INIT1;
    header.size = totalSz;

    int ret = 0;

    do {
      ssize_t writeSz = write(t->socket, (const void*)&header, sizeof(header));
      if (writeSz != sizeof(header)) 
      {
        ret = -errno; 
        break;
      }

      writeSz = write(t->readSocket, (const void*)oc, totalSz);
      if (writeSz != totalSz)
      {
        ret = -errno;
        break;
      }

      ReplResponseHeader respHeader;
      ssize_t readSz = read(t->socket, (void*)&respHeader, sizeof(respHeader));
      if (readSz != sizeof(resp))
      {
        ret = -errno;
        break;
      }
        
      ReplCursorOpenResp* resp = (ReplCursorOpenResp*)malloc(respHeader.size);
      readSz = read(t->socket, (void*)&resp, respHeader.size);
      if (readSz != respHeader.size) 
      {
        ret = -errno;
        break;
      }

      // success

    } while (0);

    return ret;
  }

  virtual void Seek(const Slice& k) override 
  {
    ReplCursorOpenReq* oc = malloc(sizeof(ReplCursorOpenReq)) + k.size();
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    memcpy(oc->buf, k.data(), k.size());

    SeekInternal(oc);
  }
  virtual void SeekToFirst() override 
  {
    ReplCursorOpenReq* oc = malloc(sizeof(ReplCursorOpenReq)) + k.size();
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekFirst = true;

    SeekInternal(oc);
  }
  virtual void SeekToLast() override 
  {
    ReplCursorOpenReq* oc = malloc(sizeof(ReplCursorOpenReq)) + k.size();
    oc->cfid = cfid_;
    oc->seq = seqnum_;
    oc->seekLast = true;

    SeekInternal(oc);
  }
  virtual void Next() override 
  {
    /*
    ReplCursorNext next;
    next.cursor_id = remote_cursor_id_;

    ReplThreadInfo* t = &repl_thread_info_;

    do {
      const ssize_t totalSz = sizeof(ReplCursorNext);

      const ssize_t writeSz = write(t->readSocket, (const void*)&next, totalSz);
      if (writeSz != totalSz)
      {
        valid_ = false;
        break;
      }

      char buf[MaxResponseSize];
      const ssize_t readSz = read(t->readSocket, (void*)buf, MaxResponseSize);

      ReplCursorNextResponse *resp = reinterpret_cast<ReplCursorNextResponse*>(buf);

      if ((readSz < (ssize_t)sizeof(resp))
       || ((readSz != (ssize_t)(sizeof(*resp) + resp->size))))
      {
        valid_ = false;
      }
      else 
      {
        if ((resp->status != Status::Code::kOk) ||
          (resp->is_eof))
        {
          valid_ = false;
        }
        else 
        {
          // copy buf
          valid_ = true;
        }
      }
    } while (0);
    */
  }
  virtual void Prev() override 
  {
    assert("not impl" == 0);
  }
  virtual Slice key() const override
  {
    return key_;
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
    return true;
  }

  private:

  ReplThreadInfo& repl_thread_info_;

  uint32_t cfid_;
  SequenceNumber seqnum_;

  int32_t remote_cursor_id_ = -1; // TODO create invalid 

  Slice key_;
  Slice value_;
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

  SequenceNumber seqnum = 0; // do we need to retrieve based on snapshot ?

  auto* arena = merge_iter_builder->GetArena();

  auto mem = arena->AllocateAligned(sizeof(ReplIterator));
  merge_iter_builder->AddIterator(
    new (mem) ReplIterator(*this, cfid, seqnum, read_options));
}

}
