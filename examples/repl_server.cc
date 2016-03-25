#include <stdio.h>
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

#include "rocksdb/types.h" // SequenceNumber
#include "rocksdb/write_batch.h" // WriteBatch
#include "db/write_batch_internal.h" // WriteBatchInternal

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

    // read socket while not eof
    if (!eof) {
      ssize_t readSz = read(newSocket, buf + readOff, sizeof(buf));
      if (readSz <= 0) {
        eof = true;
      } else {
        readOff += readSz;
      }
    }

    std::cout 
      << "read size=" << readSz 
      << "processedOff=" << processedOff 
      << "readOff=" << readOff 
      << std::endl;

    // assume multiple records are read
    // records do not get fragmented during read
    while (processedOff < readOff) {

      // assert that readOff - processedOff > value being read
      ServerWrite* sw
        = (ServerWrite*)(buf + processedOff);

      if (sw->size <= (readOff - processedOff - sizeof(ServerWrite))) {

        rocksdb::WriteBatch b;
        rocksdb::Slice slice(sw->buf, sw->size);
        rocksdb::WriteBatchInternal::SetContents(&b, slice);
  
        std::cout << "read writebatch "
          << " seq=" << sw->seq
          << " size=" << b.GetDataSize()
          << " count=" << b.Count()
          << std::endl;

        processedOff += sizeof(ServerWrite) + sw->size;
        
      } else {
        std::cout << "fragmented record read" << std::endl;
        eof = true;
        break;
      }
    }

    readOff = processedOff = 0;

  }

  return 0;
}
