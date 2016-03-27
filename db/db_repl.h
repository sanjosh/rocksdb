#pragma once

#include "rocksdb/types.h"
#include "rocksdb/status.h"

namespace rocksdb {

// communication format between rocksdb and Offloader
struct ReplWALUpdate
{
  size_t size;
  SequenceNumber seq;
  char buf[0];
};

enum ReplRequestOp
{
  OP_LOOKUP = 1,
  OP_CURSOR_OPEN = 2,
  OP_CURSOR_NEXT = 3,
  OP_CURSOR_CLOSE = 4,
};

enum ReplResponseOp
{
  RESP_LOOKUP = 1,
  RESP_CURSOR_OPEN = 2,
  RESP_CURSOR_NEXT = 3,
  RESP_CURSOR_CLOSE = 4,
};

struct ReplRequestBase
{
  ReplRequestOp op;
};

struct ReplResponseBase
{
  ReplResponseOp op;
};

struct ReplLookupRequest
{
  ReplRequestOp op = OP_LOOKUP;
  size_t size;
  uint32_t cfid; // column family id
  SequenceNumber seq;
  char buf[0];
};

struct ReplLookupResponse
{
  ReplResponseOp op = RESP_LOOKUP;
  size_t size;
  bool found;
  Status::Code status; 
  char buf[0];
};

typedef uint32_t CursorId;

struct ReplCursorOpen
{
  ReplRequestOp op = OP_CURSOR_OPEN;
  size_t size;
  uint32_t cfid;
  SequenceNumber seqnum;
  uint32_t numKeysPerNext;
  char buf[0]; // has key
};

struct ReplCursorOpenResponse
{
  ReplResponseOp op = RESP_CURSOR_OPEN;
  CursorId cursor_id;  
  Status::Code status;
};

struct ReplCursorNext
{
  ReplRequestOp op = OP_CURSOR_NEXT;
  CursorId cursor_id;
};

struct ReplCursorNextResponse
{
  ReplResponseOp op = RESP_CURSOR_NEXT;
  size_t size;
  CursorId cursor_id;
  Status::Code status;
  bool is_eof;
  char buf[0]; // has <key, seq, type, value> 
};

struct ReplCursorClose
{
  ReplRequestOp op = OP_CURSOR_CLOSE;
  CursorId cursor_id;
};

struct ReplCursorCloseResponse
{
  ReplResponseOp op = RESP_CURSOR_CLOSE;
  CursorId cursor_id;
  Status::Code status;
};

}
