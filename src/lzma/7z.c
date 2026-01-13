/* 7z.c -- 7z Archive reader implementation
   2024-01-12 : public domain
   Based on LZMA SDK by Igor Pavlov */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "7z.h"
#include "LzmaDec.h"

/* 7z signature: '7' 'z' 0xBC 0xAF 0x27 0x1C */
const Byte k7zSignature[k7zSignatureSize] = {'7', 'z', 0xBC, 0xAF, 0x27, 0x1C};

/* Property IDs for 7z headers */
#define k7zIdEnd 0x00
#define k7zIdHeader 0x01
#define k7zIdArchiveProperties 0x02
#define k7zIdAdditionalStreamsInfo 0x03
#define k7zIdMainStreamsInfo 0x04
#define k7zIdFilesInfo 0x05
#define k7zIdPackInfo 0x06
#define k7zIdUnpackInfo 0x07
#define k7zIdSubStreamsInfo 0x08
#define k7zIdSize 0x09
#define k7zIdCRC 0x0A
#define k7zIdFolder 0x0B
#define k7zIdCodersUnpackSize 0x0C
#define k7zIdNumUnpackStream 0x0D
#define k7zIdEmptyStream 0x0E
#define k7zIdEmptyFile 0x0F
#define k7zIdAnti 0x10
#define k7zIdName 0x11
#define k7zIdCTime 0x12
#define k7zIdATime 0x13
#define k7zIdMTime 0x14
#define k7zIdWinAttrib 0x15
#define k7zIdComment 0x16
#define k7zIdEncodedHeader 0x17
#define k7zIdStartPos 0x18
#define k7zIdDummy 0x19

/* Codec IDs */
#define k7zMethodIdLZMA 0x030101
#define k7zMethodIdLZMA2 0x21
#define k7zMethodIdCopy 0x00

/* Default memory allocator */
static void *SzAlloc(void *p, size_t size) { (void)p; return malloc(size); }
static void SzFree(void *p, void *address) { (void)p; free(address); }

static ISzAlloc g_Alloc = { SzAlloc, SzFree };

/* Helper functions for reading from file */
static SRes ReadByte(FILE *f, Byte *b)
{
    if (fread(b, 1, 1, f) != 1)
        return SZ_ERROR_READ;
    return SZ_OK;
}

static SRes ReadBytes(FILE *f, void *data, size_t size)
{
    if (fread(data, 1, size, f) != size)
        return SZ_ERROR_READ;
    return SZ_OK;
}

/* Read variable-length encoded number (7z uses this format) */
static SRes ReadNumber(FILE *f, UInt64 *value)
{
    Byte firstByte;
    UInt64 mask = 0x80;
    UInt64 val = 0;
    int i;
    
    if (fread(&firstByte, 1, 1, f) != 1)
        return SZ_ERROR_READ;
    
    for (i = 0; i < 8; i++)
    {
        if ((firstByte & mask) == 0)
        {
            val |= (((UInt64)firstByte & (mask - 1)) << (8 * i));
            break;
        }
        
        Byte b;
        if (fread(&b, 1, 1, f) != 1)
            return SZ_ERROR_READ;
        
        val |= ((UInt64)b << (8 * i));
        mask >>= 1;
    }
    
    *value = val;
    return SZ_OK;
}

/* Read 32-bit little-endian value */
static SRes ReadUInt32(FILE *f, UInt32 *value)
{
    Byte buf[4];
    if (fread(buf, 1, 4, f) != 4)
        return SZ_ERROR_READ;
    *value = (UInt32)buf[0] | ((UInt32)buf[1] << 8) | ((UInt32)buf[2] << 16) | ((UInt32)buf[3] << 24);
    return SZ_OK;
}

/* Read 64-bit little-endian value */
static SRes ReadUInt64(FILE *f, UInt64 *value)
{
    Byte buf[8];
    if (fread(buf, 1, 8, f) != 8)
        return SZ_ERROR_READ;
    *value = (UInt64)buf[0] | 
             ((UInt64)buf[1] << 8) | 
             ((UInt64)buf[2] << 16) | 
             ((UInt64)buf[3] << 24) |
             ((UInt64)buf[4] << 32) |
             ((UInt64)buf[5] << 40) |
             ((UInt64)buf[6] << 48) |
             ((UInt64)buf[7] << 56);
    return SZ_OK;
}

/* Helper: Read 32-bit little-endian value from buffer */
static UInt32 GetUInt32FromBuffer(const Byte *buf)
{
    return (UInt32)buf[0] | ((UInt32)buf[1] << 8) | 
           ((UInt32)buf[2] << 16) | ((UInt32)buf[3] << 24);
}

/* Helper: Read 64-bit little-endian value from buffer */
static UInt64 GetUInt64FromBuffer(const Byte *buf)
{
    return (UInt64)buf[0] | 
           ((UInt64)buf[1] << 8) | 
           ((UInt64)buf[2] << 16) | 
           ((UInt64)buf[3] << 24) |
           ((UInt64)buf[4] << 32) |
           ((UInt64)buf[5] << 40) |
           ((UInt64)buf[6] << 48) |
           ((UInt64)buf[7] << 56);
}

/* CRC32 calculation */
static UInt32 g_CrcTable[256];
static Bool g_CrcTableInitialized = False;

static void CrcGenerateTable(void)
{
    UInt32 i, j;
    for (i = 0; i < 256; i++)
    {
        UInt32 r = i;
        for (j = 0; j < 8; j++)
            r = (r >> 1) ^ (0xEDB88320 & ~((r & 1) - 1));
        g_CrcTable[i] = r;
    }
    g_CrcTableInitialized = True;
}

static UInt32 CrcCalc(const void *data, size_t size)
{
    UInt32 v = 0xFFFFFFFF;
    const Byte *p = (const Byte *)data;
    if (!g_CrcTableInitialized)
        CrcGenerateTable();
    while (size--)
        v = g_CrcTable[(Byte)(v ^ *p++)] ^ (v >> 8);
    return v ^ 0xFFFFFFFF;
}

Bool Sz7z_IsSignature(const Byte *testBytes, size_t size)
{
    if (size < k7zSignatureSize)
        return False;
    return memcmp(testBytes, k7zSignature, k7zSignatureSize) == 0;
}

void Sz7z_Init(CSz7zArchive *archive, ISzAlloc *alloc)
{
    memset(archive, 0, sizeof(*archive));
    archive->alloc = alloc ? alloc : &g_Alloc;
}

void Sz7z_Free(CSz7zArchive *archive)
{
    if (archive->files)
    {
        for (UInt32 i = 0; i < archive->numFiles; i++)
        {
            if (archive->files[i].Name)
                archive->alloc->Free(archive->alloc, archive->files[i].Name);
        }
        archive->alloc->Free(archive->alloc, archive->files);
        archive->files = NULL;
    }
    if (archive->packSizes)
    {
        archive->alloc->Free(archive->alloc, archive->packSizes);
        archive->packSizes = NULL;
    }
    if (archive->folders)
    {
        for (UInt32 i = 0; i < archive->numFolders; i++)
        {
            if (archive->folders[i].Coders)
                archive->alloc->Free(archive->alloc, archive->folders[i].Coders);
            if (archive->folders[i].UnpackSizes)
                archive->alloc->Free(archive->alloc, archive->folders[i].UnpackSizes);
        }
        archive->alloc->Free(archive->alloc, archive->folders);
        archive->folders = NULL;
    }
    if (archive->fileToFolder)
    {
        archive->alloc->Free(archive->alloc, archive->fileToFolder);
        archive->fileToFolder = NULL;
    }
    if (archive->fileIndexInFolder)
    {
        archive->alloc->Free(archive->alloc, archive->fileIndexInFolder);
        archive->fileIndexInFolder = NULL;
    }
    archive->numFiles = 0;
    archive->numPackStreams = 0;
    archive->numFolders = 0;
}

void Sz7z_Close(CSz7zArchive *archive)
{
    if (archive->file)
    {
        fclose(archive->file);
        archive->file = NULL;
    }
    Sz7z_Free(archive);
}

/* Skip remaining data in a header section */
static SRes SkipData(FILE *f, UInt64 size)
{
    return fseek(f, (long)size, SEEK_CUR) == 0 ? SZ_OK : SZ_ERROR_READ;
}

/* Structure to hold encoded header streams info */
typedef struct
{
    UInt64 packPos;         /* Offset of packed data after signature header */
    UInt64 packSize;        /* Size of compressed data */
    UInt64 unpackSize;      /* Size of uncompressed data */
    UInt32 coderMethod;     /* Codec method ID */
    Byte lzmaProps[5];      /* LZMA properties (5 bytes) */
    Bool hasProps;          /* Whether we have properties */
} CEncodedHeaderInfo;

/* Read a number from memory buffer */
static SRes ReadNumberFromBuf(const Byte **buf, const Byte *bufEnd, UInt64 *value)
{
    const Byte *p = *buf;
    Byte firstByte;
    UInt64 mask = 0x80;
    UInt64 val = 0;
    int i;
    
    if (p >= bufEnd)
        return SZ_ERROR_READ;
    
    firstByte = *p++;
    
    for (i = 0; i < 8; i++)
    {
        if ((firstByte & mask) == 0)
        {
            val |= (((UInt64)firstByte & (mask - 1)) << (8 * i));
            break;
        }
        
        if (p >= bufEnd)
            return SZ_ERROR_READ;
        
        Byte b = *p++;
        val |= ((UInt64)b << (8 * i));
        mask >>= 1;
    }
    
    *value = val;
    *buf = p;
    return SZ_OK;
}

/* Read byte from buffer */
static SRes ReadByteFromBuf(const Byte **buf, const Byte *bufEnd, Byte *value)
{
    if (*buf >= bufEnd)
        return SZ_ERROR_READ;
    *value = **buf;
    (*buf)++;
    return SZ_OK;
}

/* Read the streams info for encoded header */
static SRes ReadEncodedHeaderStreamsInfo(FILE *f, CEncodedHeaderInfo *info)
{
    Byte type;
    UInt64 numPackStreams;
    UInt64 numFolders;
    
    memset(info, 0, sizeof(*info));
    
    /* Read type - can either be k7zIdMainStreamsInfo (0x04) or start directly with PackInfo */
    RINOK(ReadByte(f, &type));
    if (type == k7zIdMainStreamsInfo) {
        /* Move to first subsection */
        RINOK(ReadByte(f, &type));
    } else if (type != k7zIdPackInfo && type != k7zIdUnpackInfo && type != k7zIdSubStreamsInfo) {
        return SZ_ERROR_ARCHIVE;
    }
    
    while (type != k7zIdEnd)
    {
        switch (type)
        {
        case k7zIdPackInfo:
            /* Read pack position */
            RINOK(ReadNumber(f, &info->packPos));
            /* Read number of pack streams */
            RINOK(ReadNumber(f, &numPackStreams));
            if (numPackStreams != 1)
                return SZ_ERROR_UNSUPPORTED;  /* Only support single stream for now */
            
            RINOK(ReadByte(f, &type));
            if (type == k7zIdSize)
            {
                RINOK(ReadNumber(f, &info->packSize));
                RINOK(ReadByte(f, &type));
            }
            if (type == k7zIdCRC)
            {
                /* Skip CRC data - first byte indicates which CRCs are defined */
                /* For single stream, skip the definition byte and the CRC itself */
                RINOK(SkipData(f, 1));  /* All defined byte */
                RINOK(SkipData(f, 4));  /* CRC value */
                RINOK(ReadByte(f, &type));
            }
            if (type != k7zIdEnd)
                return SZ_ERROR_ARCHIVE;
            break;
            
        case k7zIdUnpackInfo:
            RINOK(ReadByte(f, &type));
            if (type != k7zIdFolder)
                return SZ_ERROR_ARCHIVE;
            
            /* Read number of folders */
            RINOK(ReadNumber(f, &numFolders));
            if (numFolders != 1)
                return SZ_ERROR_UNSUPPORTED;  /* Only support single folder for now */
            
            /* External indicator (should be 0 for inline data) */
            RINOK(ReadByte(f, &type));
            if (type != 0)
                return SZ_ERROR_UNSUPPORTED;
            
            /* Read folder info - number of coders */
            {
                UInt64 numCoders;
                Byte coderInfo;
                UInt64 numInStreams = 0, numOutStreams = 0;
                
                RINOK(ReadNumber(f, &numCoders));
                if (numCoders != 1)
                    return SZ_ERROR_UNSUPPORTED;  /* Only support single coder for now */
                
                /* Read coder info */
                RINOK(ReadByte(f, &coderInfo));
                
                /* Bits 0-3: ID size */
                UInt32 idSize = (coderInfo & 0x0F);
                Bool isComplex = (coderInfo & 0x10) != 0;
                Bool hasAttributes = (coderInfo & 0x20) != 0;
                
                if (idSize > 4)
                    return SZ_ERROR_UNSUPPORTED;
                
                /* Read codec ID */
                info->coderMethod = 0;
                for (UInt32 i = 0; i < idSize; i++)
                {
                    Byte b;
                    RINOK(ReadByte(f, &b));
                    info->coderMethod = (info->coderMethod << 8) | b;
                }
                
                /* Only support LZMA for now */
                if (info->coderMethod != k7zMethodIdLZMA && info->coderMethod != k7zMethodIdCopy)
                    return SZ_ERROR_UNSUPPORTED;
                
                if (isComplex)
                {
                    RINOK(ReadNumber(f, &numInStreams));
                    RINOK(ReadNumber(f, &numOutStreams));
                }
                else
                {
                    numInStreams = 1;
                    numOutStreams = 1;
                }
                
                /* Read properties if present */
                if (hasAttributes)
                {
                    UInt64 propsSize;
                    RINOK(ReadNumber(f, &propsSize));
                    
                    if (propsSize == 5)
                    {
                        RINOK(ReadBytes(f, info->lzmaProps, 5));
                        info->hasProps = True;
                    }
                    else
                    {
                        /* Skip unsupported properties */
                        RINOK(SkipData(f, propsSize));
                    }
                }
            }
            
            /* Read unpack sizes */
            RINOK(ReadByte(f, &type));
            if (type != k7zIdCodersUnpackSize)
            {
                /* Try to find CodersUnpackSize */
                while (type != k7zIdCodersUnpackSize && type != k7zIdEnd)
                {
                    RINOK(ReadByte(f, &type));
                }
            }
            
            if (type == k7zIdCodersUnpackSize)
            {
                RINOK(ReadNumber(f, &info->unpackSize));
                RINOK(ReadByte(f, &type));
            }
            
            /* Skip remaining data until end */
            while (type != k7zIdEnd)
            {
                if (type == k7zIdCRC)
                {
                    RINOK(SkipData(f, 1));  /* All defined byte */
                    RINOK(SkipData(f, 4));  /* CRC value */
                }
                RINOK(ReadByte(f, &type));
            }
            break;
            
        default:
            /* Skip unknown sections - but we need to know the size */
            return SZ_ERROR_UNSUPPORTED;
        }
        
        RINOK(ReadByte(f, &type));
    }
    
    return SZ_OK;
}

/* Forward declarations */
static SRes ParseHeaderFromBuffer(CSz7zArchive *archive, const Byte *buf, size_t bufSize);
static SRes ParseStreamsInfoFromBuf(CSz7zArchive *archive, const Byte **bufPtr, const Byte *bufEnd);

/* Decode the encoded header and parse it */
static SRes DecodeEncodedHeader(CSz7zArchive *archive, UInt64 headerOffset)
{
    FILE *f = archive->file;
    CEncodedHeaderInfo info;
    SRes res;
    
    /* Read the streams info for the encoded header */
    res = ReadEncodedHeaderStreamsInfo(f, &info);
    if (res != SZ_OK)
        return res;
    
    /* Validate we have enough info */
    if (info.packSize == 0 || info.unpackSize == 0)
        return SZ_ERROR_ARCHIVE;
    
    /* Seek to the packed data position (relative to end of signature header = 32 bytes) */
    if (fseek(f, (long)(32 + info.packPos), SEEK_SET) != 0)
        return SZ_ERROR_READ;
    
    /* Allocate buffers */
    Byte *packedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)info.packSize);
    if (!packedData)
        return SZ_ERROR_MEM;
    
    Byte *unpackedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)info.unpackSize);
    if (!unpackedData)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_MEM;
    }
    
    /* Read packed data */
    res = ReadBytes(f, packedData, (size_t)info.packSize);
    if (res != SZ_OK)
    {
        archive->alloc->Free(archive->alloc, packedData);
        archive->alloc->Free(archive->alloc, unpackedData);
        return res;
    }
    
    /* Decompress based on method */
    if (info.coderMethod == k7zMethodIdCopy)
    {
        /* Just copy */
        if (info.packSize != info.unpackSize)
        {
            archive->alloc->Free(archive->alloc, packedData);
            archive->alloc->Free(archive->alloc, unpackedData);
            return SZ_ERROR_DATA;
        }
        memcpy(unpackedData, packedData, (size_t)info.packSize);
    }
    else if (info.coderMethod == k7zMethodIdLZMA)
    {
        if (!info.hasProps)
        {
            archive->alloc->Free(archive->alloc, packedData);
            archive->alloc->Free(archive->alloc, unpackedData);
            return SZ_ERROR_ARCHIVE;
        }
        
        /* Decompress using LZMA */
        size_t destLen = (size_t)info.unpackSize;
        size_t srcLen = (size_t)info.packSize;
        ELzmaStatus status;
        
        res = LzmaDecode(unpackedData, &destLen, packedData, &srcLen,
                        info.lzmaProps, 5, LZMA_FINISH_END, &status, archive->alloc);
        
        if (res != SZ_OK || destLen != info.unpackSize)
        {
            archive->alloc->Free(archive->alloc, packedData);
            archive->alloc->Free(archive->alloc, unpackedData);
            return res != SZ_OK ? res : SZ_ERROR_DATA;
        }
    }
    else
    {
        archive->alloc->Free(archive->alloc, packedData);
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_UNSUPPORTED;
    }
    
    /* Parse the decompressed header */
    res = ParseHeaderFromBuffer(archive, unpackedData, (size_t)info.unpackSize);
    
    /* Cleanup */
    archive->alloc->Free(archive->alloc, packedData);
    archive->alloc->Free(archive->alloc, unpackedData);
    
    return res;
}

/* Forward declarations for header parsing */
static SRes ReadPackInfo(FILE *f, CSz7zArchive *archive, UInt64 *dataOffset);
static SRes ReadUnpackInfo(FILE *f, CSz7zArchive *archive);
static SRes ReadSubStreamsInfo(FILE *f, CSz7zArchive *archive);
static SRes ReadFilesInfo(FILE *f, CSz7zArchive *archive);
static SRes ReadFilesInfoFromBuf(CSz7zArchive *archive, const Byte **buf, const Byte *bufEnd);

/* Parse streams info from buffer to get pack sizes and unpack sizes */
static SRes ParseStreamsInfoFromBuf(CSz7zArchive *archive, const Byte **bufPtr, const Byte *bufEnd)
{
    const Byte *p = *bufPtr;
    Byte type;

    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    while (type != k7zIdEnd && p < bufEnd)
    {
        switch (type)
        {
        case k7zIdPackInfo:
            {
                UInt64 packPos, numPackStreams;
                RINOK(ReadNumberFromBuf(&p, bufEnd, &packPos));
                RINOK(ReadNumberFromBuf(&p, bufEnd, &numPackStreams));

                archive->numPackStreams = (UInt32)numPackStreams;
                archive->dataOffset = 32 + packPos;

                RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                if (type == k7zIdSize)
                {
                    archive->packSizes = (UInt64 *)archive->alloc->Alloc(
                        archive->alloc, sizeof(UInt64) * archive->numPackStreams);
                    if (!archive->packSizes)
                        return SZ_ERROR_MEM;

                    for (UInt32 i = 0; i < archive->numPackStreams; i++)
                    {
                        RINOK(ReadNumberFromBuf(&p, bufEnd, &archive->packSizes[i]));
                    }
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                }
                /* Skip to end of PackInfo */
                while (type != k7zIdEnd && p < bufEnd)
                {
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                }
            }
            break;

        case k7zIdUnpackInfo:
        case k7zIdSubStreamsInfo:
            /* Skip these sections for now - we'll use file UnpackSize */
            {
                int depth = 1;
                while (depth > 0 && p < bufEnd)
                {
                    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
                    if (type == k7zIdEnd)
                        depth--;
                }
            }
            break;

        default:
            /* Unknown section, try to skip */
            break;
        }

        if (p < bufEnd)
            RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    }

    *bufPtr = p;
    return SZ_OK;
}

/* Parse header from buffer (for encoded headers) */
static SRes ParseHeaderFromBuffer(CSz7zArchive *archive, const Byte *buf, size_t bufSize)
{
    const Byte *p = buf;
    const Byte *bufEnd = buf + bufSize;
    Byte type;
    SRes res;

    /* Read header type */
    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    if (type != k7zIdHeader)
        return SZ_ERROR_ARCHIVE;

    /* Read next type */
    RINOK(ReadByteFromBuf(&p, bufEnd, &type));

    /* Parse MainStreamsInfo if present */
    if (type == k7zIdMainStreamsInfo)
    {
        res = ParseStreamsInfoFromBuf(archive, &p, bufEnd);
        if (res != SZ_OK)
            return res;
        RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    }

    /* Now parse FilesInfo */
    if (type == k7zIdFilesInfo)
    {
        return ReadFilesInfoFromBuf(archive, &p, bufEnd);
    }

    /* Fallback: scan for FilesInfo section */
    const Byte *files_pos = NULL;
    const Byte *scan;

    for (scan = buf; scan < bufEnd; scan++)
    {
        if (*scan == k7zIdFilesInfo)
        {
            files_pos = scan;
            break;
        }
    }

    if (!files_pos)
        return SZ_ERROR_ARCHIVE;

    const Byte *files_ptr = files_pos + 1;
    return ReadFilesInfoFromBuf(archive, &files_ptr, bufEnd);
}

/* Read files info from buffer */
static SRes ReadFilesInfoFromBuf(CSz7zArchive *archive, const Byte **bufPtr, const Byte *bufEnd)
{
    const Byte *p = *bufPtr;
    UInt64 numFiles;
    Byte type;
    UInt32 i;
    
    RINOK(ReadNumberFromBuf(&p, bufEnd, &numFiles));
    archive->numFiles = (UInt32)numFiles;
    
    if (archive->numFiles > 0)
    {
        archive->files = (CSz7zFileInfo *)archive->alloc->Alloc(archive->alloc,
            sizeof(CSz7zFileInfo) * archive->numFiles);
        if (!archive->files)
            return SZ_ERROR_MEM;
        memset(archive->files, 0, sizeof(CSz7zFileInfo) * archive->numFiles);
    }
    
    RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    
    while (type != k7zIdEnd && p < bufEnd)
    {
        UInt64 size;
        RINOK(ReadNumberFromBuf(&p, bufEnd, &size));
        
        if (p + size > bufEnd)
            return SZ_ERROR_READ;
        
        const Byte *sectionEnd = p + size;
        
        switch (type)
        {
        case k7zIdEmptyStream:
        case k7zIdEmptyFile:
        case k7zIdAnti:
            /* Skip for now */
            p = sectionEnd;
            break;
            
        case k7zIdName:
            {
                Byte external;
                if (p >= sectionEnd)
                    return SZ_ERROR_READ;
                external = *p++;
                
                if (external != 0)
                {
                    p = sectionEnd;
                }
                else
                {
                    /* Read file names (UTF-16LE encoded) */
                    size_t namesDataSize = sectionEnd - p;
                    const Byte *namesData = p;
                    
                    /* Parse names - each name is null-terminated UTF-16LE */
                    size_t pos = 0;
                    for (i = 0; i < archive->numFiles && pos < namesDataSize; i++)
                    {
                        /* Find end of this name */
                        size_t nameStart = pos;
                        while (pos + 1 < namesDataSize)
                        {
                            if (namesData[pos] == 0 && namesData[pos + 1] == 0)
                            {
                                pos += 2;
                                break;
                            }
                            pos += 2;
                        }
                        
                        /* Convert UTF-16LE to ASCII (simplified conversion) */
                        size_t nameLen = (pos - nameStart) / 2 - 1;
                        archive->files[i].Name = (char *)archive->alloc->Alloc(archive->alloc, nameLen + 1);
                        if (archive->files[i].Name)
                        {
                            size_t j;
                            for (j = 0; j < nameLen; j++)
                            {
                                UInt16 c = namesData[nameStart + j * 2] | 
                                          ((UInt16)namesData[nameStart + j * 2 + 1] << 8);
                                /* ASCII-only conversion - non-ASCII chars become '?' */
                                archive->files[i].Name[j] = (c < 128) ? (char)c : '?';
                            }
                            archive->files[i].Name[nameLen] = '\0';
                            archive->files[i].NameLen = (UInt32)nameLen;
                        }
                    }
                    
                    p = sectionEnd;
                }
            }
            break;
            
        case k7zIdMTime:
        case k7zIdCTime:
        case k7zIdATime:
        case k7zIdWinAttrib:
            /* Skip */
            p = sectionEnd;
            break;
            
        default:
            p = sectionEnd;
            break;
        }
        
        if (p >= bufEnd)
            break;
        RINOK(ReadByteFromBuf(&p, bufEnd, &type));
    }
    
    *bufPtr = p;
    return SZ_OK;
}

/* Parse the 7z header structure - simplified version */
static SRes ParseHeader(CSz7zArchive *archive, UInt64 headerOffset, UInt64 headerSize)
{
    FILE *f = archive->file;
    Byte type;
    SRes res;
    UInt64 dataOffset = 0;
    
    /* Seek to header position */
    if (fseek(f, (long)(32 + headerOffset), SEEK_SET) != 0)
        return SZ_ERROR_READ;
    
    /* Read header type */
    RINOK(ReadByte(f, &type));
    
    /* Handle encoded header (the header itself is compressed) */
    if (type == k7zIdEncodedHeader)
    {
        /* Decode the encoded header using LZMA */
        return DecodeEncodedHeader(archive, headerOffset);
    }
    
    if (type != k7zIdHeader)
        return SZ_ERROR_ARCHIVE;
    
    /* Read full header into memory for robust parsing */
    if (headerSize == 0)
        return SZ_ERROR_ARCHIVE;
    
    Byte *headerBuf = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)headerSize);
    if (!headerBuf)
        return SZ_ERROR_MEM;
    
    headerBuf[0] = type;
    size_t bytes_to_read = headerSize > 1 ? (size_t)(headerSize - 1) : 0;
    if (bytes_to_read > 0)
    {
        res = ReadBytes(f, headerBuf + 1, bytes_to_read);
        if (res != SZ_OK)
        {
            archive->alloc->Free(archive->alloc, headerBuf);
            return res;
        }
    }
    
    res = ParseHeaderFromBuffer(archive, headerBuf, (size_t)headerSize);
    archive->alloc->Free(archive->alloc, headerBuf);
    if (res != SZ_OK)
        return res;
    
    archive->dataOffset = 32 + dataOffset;
    
    return SZ_OK;
}

static SRes ReadPackInfo(FILE *f, CSz7zArchive *archive, UInt64 *dataOffset)
{
    Byte type;
    UInt64 numPackStreams;
    UInt32 i;
    
    /* Read pack position (offset from after start header) */
    RINOK(ReadNumber(f, dataOffset));
    
    /* Read number of pack streams */
    RINOK(ReadNumber(f, &numPackStreams));
    archive->numPackStreams = (UInt32)numPackStreams;
    
    RINOK(ReadByte(f, &type));
    
    if (type == k7zIdSize)
    {
        archive->packSizes = (UInt64 *)archive->alloc->Alloc(archive->alloc, 
            sizeof(UInt64) * archive->numPackStreams);
        if (!archive->packSizes)
            return SZ_ERROR_MEM;
        
        for (i = 0; i < archive->numPackStreams; i++)
        {
            RINOK(ReadNumber(f, &archive->packSizes[i]));
        }
        
        RINOK(ReadByte(f, &type));
    }
    
    if (type == k7zIdCRC)
    {
        /* Skip CRC info for now */
        UInt32 numDefined = (archive->numPackStreams + 7) / 8;
        Byte *defined = (Byte *)archive->alloc->Alloc(archive->alloc, numDefined);
        if (defined)
        {
            RINOK(ReadBytes(f, defined, numDefined));
            
            /* Count defined CRCs and skip them */
            UInt32 count = 0;
            for (i = 0; i < archive->numPackStreams; i++)
            {
                if (defined[i / 8] & (1 << (i % 8)))
                    count++;
            }
            RINOK(SkipData(f, count * 4)); /* 4 bytes per CRC */
            
            archive->alloc->Free(archive->alloc, defined);
        }
        
        RINOK(ReadByte(f, &type));
    }
    
    if (type != k7zIdEnd)
        return SZ_ERROR_ARCHIVE;
    
    return SZ_OK;
}

static SRes ReadUnpackInfo(FILE *f, CSz7zArchive *archive)
{
    Byte type;
    
    RINOK(ReadByte(f, &type));
    
    while (type != k7zIdEnd)
    {
        UInt64 size;
        RINOK(ReadNumber(f, &size));
        RINOK(SkipData(f, size));
        RINOK(ReadByte(f, &type));
    }
    
    return SZ_OK;
}

static SRes ReadSubStreamsInfo(FILE *f, CSz7zArchive *archive)
{
    Byte type;
    
    RINOK(ReadByte(f, &type));
    
    while (type != k7zIdEnd)
    {
        UInt64 size;
        RINOK(ReadNumber(f, &size));
        RINOK(SkipData(f, size));
        RINOK(ReadByte(f, &type));
    }
    
    return SZ_OK;
}

static SRes ReadFilesInfo(FILE *f, CSz7zArchive *archive)
{
    UInt64 numFiles;
    Byte type;
    UInt32 i;
    
    RINOK(ReadNumber(f, &numFiles));
    archive->numFiles = (UInt32)numFiles;
    
    if (archive->numFiles > 0)
    {
        archive->files = (CSz7zFileInfo *)archive->alloc->Alloc(archive->alloc,
            sizeof(CSz7zFileInfo) * archive->numFiles);
        if (!archive->files)
            return SZ_ERROR_MEM;
        memset(archive->files, 0, sizeof(CSz7zFileInfo) * archive->numFiles);
    }
    
    RINOK(ReadByte(f, &type));
    
    while (type != k7zIdEnd)
    {
        UInt64 size;
        RINOK(ReadNumber(f, &size));
        
        switch (type)
        {
        case k7zIdEmptyStream:
        case k7zIdEmptyFile:
        case k7zIdAnti:
            /* Skip for now */
            RINOK(SkipData(f, size));
            break;
            
        case k7zIdName:
            {
                Byte external;
                RINOK(ReadByte(f, &external));
                
                if (external != 0)
                {
                    RINOK(SkipData(f, size - 1));
                }
                else
                {
                    /* Read file names (UTF-16LE encoded) */
                    UInt64 namesDataSize = size - 1;
                    Byte *namesData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)namesDataSize);
                    if (!namesData)
                        return SZ_ERROR_MEM;
                    
                    RINOK(ReadBytes(f, namesData, (size_t)namesDataSize));
                    
                    /* Parse names - each name is null-terminated UTF-16LE */
                    size_t pos = 0;
                    for (i = 0; i < archive->numFiles && pos < namesDataSize; i++)
                    {
                        /* Find end of this name */
                        size_t nameStart = pos;
                        while (pos + 1 < namesDataSize)
                        {
                            if (namesData[pos] == 0 && namesData[pos + 1] == 0)
                            {
                                pos += 2;
                                break;
                            }
                            pos += 2;
                        }
                        
                        /* Convert UTF-16LE to ASCII (simplified conversion)
                         * Note: Non-ASCII characters (including accented letters, CJK characters,
                         * and other Unicode characters) are replaced with '?' for simplicity.
                         * A full implementation would require proper UTF-16 to UTF-8 conversion.
                         */
                        size_t nameLen = (pos - nameStart) / 2 - 1;
                        archive->files[i].Name = (char *)archive->alloc->Alloc(archive->alloc, nameLen + 1);
                        if (archive->files[i].Name)
                        {
                            size_t j;
                            for (j = 0; j < nameLen; j++)
                            {
                                UInt16 c = namesData[nameStart + j * 2] | 
                                          ((UInt16)namesData[nameStart + j * 2 + 1] << 8);
                                /* ASCII-only conversion - non-ASCII chars become '?' */
                                archive->files[i].Name[j] = (c < 128) ? (char)c : '?';
                            }
                            archive->files[i].Name[nameLen] = '\0';
                            archive->files[i].NameLen = (UInt32)nameLen;
                        }
                    }
                    
                    archive->alloc->Free(archive->alloc, namesData);
                }
            }
            break;
            
        case k7zIdMTime:
        case k7zIdCTime:
        case k7zIdATime:
            /* Skip timestamp data */
            RINOK(SkipData(f, size));
            break;
            
        case k7zIdWinAttrib:
            /* Skip attributes */
            RINOK(SkipData(f, size));
            break;
            
        default:
            RINOK(SkipData(f, size));
            break;
        }
        
        RINOK(ReadByte(f, &type));
    }
    
    return SZ_OK;
}

SRes Sz7z_Open(CSz7zArchive *archive, const char *path)
{
    Byte header[32];
    UInt64 nextHeaderOffset;
    UInt64 nextHeaderSize;
    UInt32 nextHeaderCRC;
    SRes res;
    
    /* Initialize CRC table */
    if (!g_CrcTableInitialized)
        CrcGenerateTable();
    
    /* Open file */
    archive->file = fopen(path, "rb");
    if (!archive->file)
        return SZ_ERROR_READ;
    
    /* Get file size */
    fseek(archive->file, 0, SEEK_END);
    archive->archiveSize = (UInt64)ftell(archive->file);
    fseek(archive->file, 0, SEEK_SET);
    
    /* Read start header (32 bytes) */
    if (fread(header, 1, 32, archive->file) != 32)
    {
        Sz7z_Close(archive);
        return SZ_ERROR_READ;
    }
    
    /* Verify signature */
    if (!Sz7z_IsSignature(header, k7zSignatureSize))
    {
        Sz7z_Close(archive);
        return SZ_ERROR_NO_ARCHIVE;
    }
    
    /* Check version */
    if (header[6] > k7zMajorVersion)
    {
        Sz7z_Close(archive);
        return SZ_ERROR_UNSUPPORTED;
    }
    
    /* Read next header info using helper functions */
    nextHeaderOffset = GetUInt64FromBuffer(&header[12]);
    nextHeaderSize = GetUInt64FromBuffer(&header[20]);
    nextHeaderCRC = GetUInt32FromBuffer(&header[28]);
    
    /* Suppress unused variable warning */
    (void)nextHeaderCRC;
    
    /* Parse the header */
    res = ParseHeader(archive, nextHeaderOffset, nextHeaderSize);
    if (res != SZ_OK)
    {
        Sz7z_Close(archive);
        return res;
    }
    
    return SZ_OK;
}

UInt32 Sz7z_GetNumFiles(const CSz7zArchive *archive)
{
    return archive->numFiles;
}

const CSz7zFileInfo* Sz7z_GetFileInfo(const CSz7zArchive *archive, UInt32 index)
{
    if (index >= archive->numFiles)
        return NULL;
    return &archive->files[index];
}

SRes Sz7z_Extract(CSz7zArchive *archive, UInt32 fileIndex,
                   Byte **outBuf, size_t *outSize)
{
    if (!archive || !outBuf || !outSize)
        return SZ_ERROR_PARAM;

    if (fileIndex >= archive->numFiles)
        return SZ_ERROR_PARAM;

    const CSz7zFileInfo *fileInfo = &archive->files[fileIndex];

    /* Directories have no content */
    if (fileInfo->IsDir)
    {
        *outBuf = NULL;
        *outSize = 0;
        return SZ_OK;
    }

    /* For files with zero size, return empty buffer */
    if (fileInfo->UnpackSize == 0)
    {
        *outBuf = (Byte *)archive->alloc->Alloc(archive->alloc, 1);
        if (!*outBuf)
            return SZ_ERROR_MEM;
        (*outBuf)[0] = 0;
        *outSize = 0;
        return SZ_OK;
    }

    /* Read packed data from archive */
    if (!archive->file)
        return SZ_ERROR_READ;

    /* Calculate offset to packed data for this file */
    /* In a simple 7z archive, packed data starts at dataOffset */
    UInt64 packOffset = archive->dataOffset;
    UInt64 packSize = 0;

    /* Sum pack sizes to get total compressed size */
    if (archive->packSizes != NULL)
    {
        for (UInt32 i = 0; i < archive->numPackStreams; i++)
        {
            packSize += archive->packSizes[i];
        }
    }

    /* If no pack info, try to calculate from file positions */
    if (packSize == 0)
    {
        /* Fallback: estimate from archive size minus header */
        packSize = archive->archiveSize - packOffset;
    }

    /* Seek to packed data */
    if (fseek(archive->file, (long)packOffset, SEEK_SET) != 0)
        return SZ_ERROR_READ;

    /* Allocate buffer for packed data */
    Byte *packedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)packSize);
    if (!packedData)
        return SZ_ERROR_MEM;

    /* Read packed data */
    if (fread(packedData, 1, (size_t)packSize, archive->file) != (size_t)packSize)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_READ;
    }

    /* Calculate total unpack size for solid block */
    UInt64 totalUnpackSize = 0;
    for (UInt32 i = 0; i < archive->numFiles; i++)
    {
        if (!archive->files[i].IsDir)
            totalUnpackSize += archive->files[i].UnpackSize;
    }

    if (totalUnpackSize == 0)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_DATA;
    }

    /* Allocate output buffer for full decompression */
    Byte *unpackedData = (Byte *)archive->alloc->Alloc(archive->alloc, (size_t)totalUnpackSize);
    if (!unpackedData)
    {
        archive->alloc->Free(archive->alloc, packedData);
        return SZ_ERROR_MEM;
    }

    /* Try LZMA decompression */
    /* LZMA stream format: 5-byte props + compressed data */
    if (packSize < 5)
    {
        archive->alloc->Free(archive->alloc, packedData);
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_DATA;
    }

    size_t destLen = (size_t)totalUnpackSize;
    size_t srcLen = (size_t)packSize - 5;  /* Exclude 5-byte props header */
    ELzmaStatus status;

    SRes res = LzmaDecode(unpackedData, &destLen,
                          packedData + 5, &srcLen,
                          packedData, 5,  /* Props are first 5 bytes */
                          LZMA_FINISH_END, &status, archive->alloc);

    archive->alloc->Free(archive->alloc, packedData);

    if (res != SZ_OK)
    {
        archive->alloc->Free(archive->alloc, unpackedData);
        return res;
    }

    /* Find offset for requested file in unpacked stream */
    UInt64 fileOffset = 0;
    for (UInt32 i = 0; i < fileIndex; i++)
    {
        if (!archive->files[i].IsDir)
            fileOffset += archive->files[i].UnpackSize;
    }

    /* Allocate and copy file data */
    *outSize = (size_t)fileInfo->UnpackSize;

    if (fileOffset + fileInfo->UnpackSize > totalUnpackSize)
    {
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_DATA;
    }

    *outBuf = (Byte *)archive->alloc->Alloc(archive->alloc, *outSize + 1);
    if (!*outBuf)
    {
        archive->alloc->Free(archive->alloc, unpackedData);
        return SZ_ERROR_MEM;
    }

    memcpy(*outBuf, unpackedData + fileOffset, *outSize);
    (*outBuf)[*outSize] = 0;  /* Null terminate for text files */

    archive->alloc->Free(archive->alloc, unpackedData);

    return SZ_OK;
}
