#include <stdio.h>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <map> // inmemory map
#include <string> // key value

#include "rocksdb/types.h" // SequenceNumber
#include "rocksdb/status.h" // Status
#include "rocksdb/slice.h" // Slice
#include "rocksdb/write_batch.h" // WriteBatch
#include "db/write_batch_internal.h" // WriteBatchInternal

// need to add KeyType and SequenceNumber
typedef std::map<std::string, std::string> InMemKV;
typedef std::map<uint32_t, InMemKV> InMemDB;

using rocksdb::WriteBatch;
using rocksdb::Slice;
using rocksdb::Status;

struct MapInserter : public WriteBatch::Handler {

  InMemDB& db_;

  MapInserter() = delete;

  explicit MapInserter(InMemDB& db) : db_(db) {}

  virtual Status PutCF(uint32_t column_family_id, 
    const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf=" << column_family_id 
      << ":key=" << key.ToString()
      << std::endl;

    auto& kvmap = db_[column_family_id];
    kvmap.insert(std::make_pair(key.ToString(), value.ToString()));
    return Status::OK();
  }

  virtual void Put(const Slice& key,
    const Slice& value) override
  {
    std::cout << "inserting into cf=0" 
      << ":key=" << key.ToString()
      << std::endl;

    auto& kvmap = db_[0];
    kvmap.insert(std::make_pair(key.ToString(), value.ToString()));
  }

  virtual Status DeleteCF(uint32_t column_family_id, 
    const Slice& key)
  {
    std::cout << "deleting from cf=" << column_family_id
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_[column_family_id];
    auto numErased = kvmap.erase(key.ToString());
    return Status::OK();
  }

  virtual void Delete(const Slice& key)
  {
    std::cout << "deleting from cf=0" 
      << ":key=" << key.ToString()
      << std::endl;
    auto& kvmap = db_[0];
    auto numErased = kvmap.erase(key.ToString());
  }

  virtual void LogData(const Slice& blob)
  {
    // TODO later
  }

};

InMemDB db;

int main(){

  int listenSocket, newSocket;
  struct sockaddr_in serverAddr;
  struct sockaddr_storage serverStorage;
  socklen_t addr_size;

  /*---- Create the socket. The three arguments are: ----*/
  /* 1) Internet domain 2) Stream socket 3) Default protocol (TCP in this case) */
  listenSocket = socket(PF_INET, SOCK_STREAM, 0);
  
  /*---- Configure settings of the server address struct ----*/
  /* Address family = Internet */
  serverAddr.sin_family = AF_INET;
  /* Set port number, using htons function to use proper byte order */
  serverAddr.sin_port = htons(8192);
  /* Set IP address to localhost */
  serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  /* Set all bits of the padding field to 0 */
  memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);  

  /*---- Bind the address struct to the socket ----*/
  bind(listenSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));

  /*---- Listen on the socket, with 5 max connection requests queued ----*/
  if(listen(listenSocket,5)==0)
    printf("Listening\n");
  else
    printf("Error\n");

  /*---- Accept call creates a new socket for the incoming connection ----*/
  addr_size = sizeof serverStorage;
  newSocket = accept(listenSocket, (struct sockaddr *) &serverStorage, &addr_size);

  char buf[8192];
  off_t processedOff = 0;
  off_t readOff = 0;
  bool eof = false;

  bzero(buf, sizeof(buf));

  struct ServerWrite
  {
    size_t size;
    rocksdb::SequenceNumber seq;
    char buf[0];
  };

  while (!eof) {

    ssize_t readSz = 0;
    // read from socket until eof
    if (!eof) {
      readSz = read(newSocket, buf + readOff, sizeof(buf));
      if (readSz <= 0) {
        eof = true;
      } else {
        readOff += readSz;
      }
    }

    std::cout 
      << "read from socket size=" << readSz 
      << ":processedOff=" << processedOff 
      << ":readOff=" << readOff 
      << std::endl;

    // for now,
    // assume multiple records are read from socket
    // no record gets fragmented during read
    while (processedOff < readOff) {

      // assert that readOff - processedOff > value being read
      ServerWrite* sw
        = (ServerWrite*)(buf + processedOff);

      if (sw->size <= (readOff - processedOff - sizeof(ServerWrite))) {

        rocksdb::WriteBatch batch;
        rocksdb::Slice slice(sw->buf, sw->size);
        rocksdb::WriteBatchInternal::SetContents(&batch, slice);
  
        std::cout << "Got a WriteBatch"
          << " seq=" << sw->seq
          << ":size=" << batch.GetDataSize()
          << ":num updates in batch=" << batch.Count()
          << std::endl;

        processedOff += sizeof(ServerWrite) + sw->size;

        MapInserter handler(db);
        batch.Iterate(&handler);
        
      } else {
        std::cout << "fragmented record read" << std::endl;
        eof = true;
        break;
      }
    }

    readOff = processedOff = 0;
  }

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
