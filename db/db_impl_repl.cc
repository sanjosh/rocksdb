#include "db/db_impl.h"
#include "db/auto_roll_logger.h"
#include "db/write_batch_internal.h" // ReplServerBlock
#include "rocksdb/env.h" // Env
#include "rocksdb/status.h" // Env

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
  DBImpl::ReplThreadInfo* t = reinterpret_cast<DBImpl::ReplThreadInfo*>(arg);
  t->started.store(true, std::memory_order_release);

  auto& logger = t->db->db_options_.info_log;
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
      ssize_t totalSz = sizeof(ReplServerBlock) + batch.size();

      // TODO : combine into an operator new + ctor
      ReplServerBlock* sw = (ReplServerBlock*) malloc(totalSz);
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

}
