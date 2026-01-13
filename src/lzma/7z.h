/* 7z.h -- 7z archive reader interface
   2024-01-12 : public domain */

#ifndef LZMA_7Z_H
#define LZMA_7Z_H

#include "7zTypes.h"
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define k7zSignatureSize 6
#define k7zStartHeaderSize 20
#define k7zMajorVersion 0
#define k7zMinorVersion 4

/* Archive header signature: '7' 'z' 0xBC 0xAF 0x27 0x1C */
extern const Byte k7zSignature[k7zSignatureSize];

/* File info stored for each file in the archive */
typedef struct
{
    UInt64 UnpackSize;
    UInt64 PackedSize;
    UInt32 CRC;
    Bool IsCRCDefined;
    Bool IsDir;
    char *Name;
    UInt32 NameLen;
    UInt64 MTime;      /* modification time */
    UInt32 Attrib;     /* file attributes */
    Bool AttribDefined;
} CSz7zFileInfo;

/* Coder info for decompression */
typedef struct
{
    UInt32 MethodID;        /* Compression method ID */
    Byte Props[16];         /* Coder properties (max 16 bytes) */
    UInt32 PropsSize;       /* Size of properties */
    UInt32 NumInStreams;
    UInt32 NumOutStreams;
} CSz7zCoder;

/* Folder represents a compression unit */
typedef struct
{
    CSz7zCoder *Coders;
    UInt32 NumCoders;
    UInt64 *UnpackSizes;    /* Unpack size per output stream */
    UInt32 NumUnpackStreams;
    UInt32 UnpackCRCDefined;
    UInt32 UnpackCRC;
} CSz7zFolder;

/* Main archive structure */
typedef struct
{
    FILE *file;                     /* File handle */
    UInt64 archiveSize;            /* Total archive size */
    UInt64 dataOffset;             /* Offset where packed streams start */
    
    /* Files info */
    CSz7zFileInfo *files;
    UInt32 numFiles;
    
    /* Packed streams info */
    UInt64 *packSizes;
    UInt32 numPackStreams;

    /* Folders info */
    CSz7zFolder *folders;
    UInt32 numFolders;

    /* File to folder mapping */
    UInt32 *fileToFolder;      /* Which folder contains each file */
    UInt32 *fileIndexInFolder; /* Index within the folder's output */

    /* Memory allocator */
    ISzAlloc *alloc;
} CSz7zArchive;

/* Initialize archive structure */
void Sz7z_Init(CSz7zArchive *archive, ISzAlloc *alloc);

/* Free archive structure */
void Sz7z_Free(CSz7zArchive *archive);

/* Open 7z archive from file path */
SRes Sz7z_Open(CSz7zArchive *archive, const char *path);

/* Close archive */
void Sz7z_Close(CSz7zArchive *archive);

/* Get number of files */
UInt32 Sz7z_GetNumFiles(const CSz7zArchive *archive);

/* Get file info */
const CSz7zFileInfo* Sz7z_GetFileInfo(const CSz7zArchive *archive, UInt32 index);

/* Extract file to memory - returns allocated buffer that must be freed */
SRes Sz7z_Extract(CSz7zArchive *archive, UInt32 fileIndex, 
                   Byte **outBuf, size_t *outSize);

/* Check if file signature is 7z */
Bool Sz7z_IsSignature(const Byte *testBytes, size_t size);

#ifdef __cplusplus
}
#endif

#endif /* LZMA_7Z_H */
