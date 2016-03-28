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
    return errno;
  }
  return err;
}

void DBImpl::ReplThreadBody(void* arg)
{
  ReplThreadInfo* t = reinterpret_cast<ReplThreadInfo*>(arg);
  t->started.store(true, std::memory_order_release);

  auto& logger = t->info_log;
  Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread started");

  int err = ConnectSocket(t->addr, t->port, t->socket);
  int err2 = ConnectSocket(t->addr, t->port + 1, t->readSocket);

  if ((err < 0) || (err2 < 0))
  {
    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "sockets could not start to %s:%d error=%d:%d",
      t->addr.c_str(), 
      t->port,
      err,
      err2);

    t->has_stopped.store(true, std::memory_order_release);
    Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread exiting");
    return;
  }

  std::unique_ptr<TransactionLogIterator> iter;
  SequenceNumber currentSeqNum = 0;

  while (!t->stop.load(std::memory_order_acquire)) {
      
    iter.reset();
    Log(InfoLogLevel::INFO_LEVEL, logger, 
      "Repl thread asking for logs from seq=%llu",
      currentSeqNum + 1);
        
    while (!t->db->GetUpdatesSince(currentSeqNum + 1, &iter).ok()) {
      if (!t->stop.load(std::memory_order_acquire)) {
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
      sw->size = batch.size();
      sw->seq = res.sequence;

      // When you commit a WriteBatch at seq=M with (say) 3 upd
      // rocksdb increments sequence number by 3 to M+3
      // i.e. intermediate numbers are skipped
      // Now, if you ask for a WriteBatch starting from seq M+1
      // GetUpdatesSince() will return the update with seq=M even 
      // though it is less than what you asked, because one of 
      // the updates actually havs seq=M
      currentSeqNum = res.sequence + res.writeBatchPtr->Count() - 1;

      const ssize_t writeSz = write(t->socket, (const void*)sw, totalSz);
      const int capture_errno = errno;

      if (writeSz != totalSz) {
        Log(InfoLogLevel::ERROR_LEVEL, logger, 
          "write failed sz=%ld expected=%ld errno=%d", 
          writeSz, 
          totalSz,
          capture_errno
        );
      }
      
      free(sw);

      Log(InfoLogLevel::INFO_LEVEL, logger, 
        "Repl thread sent %ld seq=%llu actual batch=%lu numUpd=%d ", 
        writeSz,
        res.sequence,
        batch.size(),
        res.writeBatchPtr->Count()
      );
    }
  }
  t->has_stopped.store(true, std::memory_order_release);
  Log(InfoLogLevel::INFO_LEVEL, logger, "Repl thread exiting");
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

  do {
    const ssize_t totalSz = sizeof(ReplLookupRequest) + key.size();

    ReplLookupRequest* lreq = (ReplLookupRequest*) malloc(totalSz);
    lreq->size = key.size();
    lreq->cfid = column_family->GetID();
    memcpy(lreq->buf, key.data(), lreq->size);
    lreq->seq = seq;

    const ssize_t writeSz = write(t->readSocket, (const void*)lreq, totalSz);

    free((void*)lreq);

    if (writeSz != totalSz) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl socket write failed");
      status = Status::IOError("Repl socket write failed");
      break;
    }

    char response[8192]; // max response size?

    const ssize_t readSz = read(t->readSocket, (void*)response, totalSz);

    if (readSz < (ssize_t)sizeof(ReplLookupResponse)) {
      Log(InfoLogLevel::ERROR_LEVEL, logger, "Repl socket read failed");
      status = Status::IOError("Repl socket read failed");
      break;
    }

    ReplLookupResponse *lresp = reinterpret_cast<ReplLookupResponse*>(response);

    if (lresp->found) {
      if (value_found) {
        *value_found = lresp->found; 
      }
      if (value) {
        value->assign(lresp->buf, lresp->size);
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
    const ReadOptions& read_options)
    : repl_thread_info_(repl_thread_info)
    , read_options_(read_options)
  {
  }

  ~ReplIterator() 
  {
  }

  virtual bool Valid() const override 
  {
    return valid_;
  }

  virtual void Seek(const Slice& k) override 
  {
  }
  virtual void SeekToFirst() override 
  {
  }
  virtual void SeekToLast() override 
  {
  }
  virtual void Next() override 
  {
  }
  virtual void Prev() override 
  {
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
    return Status::OK();
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

  Slice key_;
  Slice value_;

  ReplThreadInfo& repl_thread_info_;
  ReadOptions read_options_;

  bool valid_;

  ReplIterator(const ReplIterator& );
  void operator =(const ReplIterator& );


};

void ReplThreadInfo::AddIterators(const ReadOptions& read_options,
                           const EnvOptions& soptions,
                           MergeIteratorBuilder* merge_iter_builder) {

  auto* arena = merge_iter_builder->GetArena();

  auto mem = arena->AllocateAligned(sizeof(ReplIterator));
  merge_iter_builder->AddIterator(
    new (mem) ReplIterator(*this, read_options));
}

}
