/* 7zTypes.h -- LZMA SDK basic types
   2024-01-12 : public domain */

#ifndef LZMA_7ZTYPES_H
#define LZMA_7ZTYPES_H

#include <stddef.h>
#include <stdint.h>

typedef uint8_t Byte;
typedef uint16_t UInt16;
typedef uint32_t UInt32;
typedef uint64_t UInt64;
typedef int32_t Int32;
typedef int64_t Int64;

typedef int Bool;
#define True 1
#define False 0

typedef int SRes;
#define SZ_OK 0
#define SZ_ERROR_DATA 1
#define SZ_ERROR_MEM 2
#define SZ_ERROR_CRC 3
#define SZ_ERROR_UNSUPPORTED 4
#define SZ_ERROR_PARAM 5
#define SZ_ERROR_INPUT_EOF 6
#define SZ_ERROR_OUTPUT_EOF 7
#define SZ_ERROR_READ 8
#define SZ_ERROR_WRITE 9
#define SZ_ERROR_PROGRESS 10
#define SZ_ERROR_FAIL 11
#define SZ_ERROR_THREAD 12
#define SZ_ERROR_ARCHIVE 16
#define SZ_ERROR_NO_ARCHIVE 17

#define RINOK(x) { SRes __result__ = (x); if (__result__ != SZ_OK) return __result__; }

typedef void * (*ISzAlloc_Alloc)(void *p, size_t size);
typedef void (*ISzAlloc_Free)(void *p, void *address);

typedef struct ISzAlloc
{
  ISzAlloc_Alloc Alloc;
  ISzAlloc_Free Free;
} ISzAlloc;

typedef const ISzAlloc * ISzAllocPtr;

#ifndef MY_NO_INLINE
#ifdef _MSC_VER
#define MY_NO_INLINE __declspec(noinline)
#else
#define MY_NO_INLINE
#endif
#endif

#ifndef MY_FORCE_INLINE
#ifdef _MSC_VER
#define MY_FORCE_INLINE __forceinline
#else
#define MY_FORCE_INLINE __inline
#endif
#endif

#ifndef MY_CDECL
#ifdef _MSC_VER
#define MY_CDECL __cdecl
#else
#define MY_CDECL
#endif
#endif

#ifndef MY_FAST_CALL
#if defined(_MSC_VER) && !defined(_WIN64)
#define MY_FAST_CALL __fastcall
#else
#define MY_FAST_CALL
#endif
#endif

/* Lookup table routines */
#ifndef MY_CPU_LE
  #if defined(__i386) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64) || defined(_M_AMD64)
    #define MY_CPU_LE
  #endif
#endif

#ifndef MY_CPU_LE
  #if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    #define MY_CPU_LE
  #endif
#endif

#endif /* LZMA_7ZTYPES_H */
