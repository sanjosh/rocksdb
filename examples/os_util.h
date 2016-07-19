#pragma once
// To define gettid()
#include <sys/types.h>
#include <syscall.h>
#include <unistd.h>
#define gettid() (syscall(SYS_gettid))
// end define gettid()
//
//
