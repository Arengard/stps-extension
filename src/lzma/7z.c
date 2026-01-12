/* 7z.c -- 7z Archive reader implementation
   2024-01-12 : public domain
   Based on LZMA SDK by Igor Pavlov */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "7z.h"

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
    archive->numFiles = 0;
    archive->numPackStreams = 0;
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

/* Forward declarations for header parsing */
static SRes ReadPackInfo(FILE *f, CSz7zArchive *archive, UInt64 *dataOffset);
static SRes ReadUnpackInfo(FILE *f, CSz7zArchive *archive);
static SRes ReadSubStreamsInfo(FILE *f, CSz7zArchive *archive);
static SRes ReadFilesInfo(FILE *f, CSz7zArchive *archive);

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
        /* For now, we'll return an error for encoded headers
           as they require recursive decompression */
        return SZ_ERROR_UNSUPPORTED;
    }
    
    if (type != k7zIdHeader)
        return SZ_ERROR_ARCHIVE;
    
    /* Read main header sections */
    RINOK(ReadByte(f, &type));
    
    while (type != k7zIdEnd)
    {
        switch (type)
        {
        case k7zIdArchiveProperties:
            {
                UInt64 size;
                RINOK(ReadNumber(f, &size));
                RINOK(SkipData(f, size));
            }
            break;
            
        case k7zIdMainStreamsInfo:
            {
                Byte subType;
                RINOK(ReadByte(f, &subType));
                
                while (subType != k7zIdEnd)
                {
                    switch (subType)
                    {
                    case k7zIdPackInfo:
                        res = ReadPackInfo(f, archive, &dataOffset);
                        if (res != SZ_OK) return res;
                        break;
                        
                    case k7zIdUnpackInfo:
                        res = ReadUnpackInfo(f, archive);
                        if (res != SZ_OK) return res;
                        break;
                        
                    case k7zIdSubStreamsInfo:
                        res = ReadSubStreamsInfo(f, archive);
                        if (res != SZ_OK) return res;
                        break;
                        
                    default:
                        /* Skip unknown section */
                        {
                            UInt64 size;
                            RINOK(ReadNumber(f, &size));
                            RINOK(SkipData(f, size));
                        }
                        break;
                    }
                    RINOK(ReadByte(f, &subType));
                }
            }
            break;
            
        case k7zIdFilesInfo:
            res = ReadFilesInfo(f, archive);
            if (res != SZ_OK) return res;
            break;
            
        default:
            /* Skip unknown section */
            {
                UInt64 size;
                RINOK(ReadNumber(f, &size));
                RINOK(SkipData(f, size));
            }
            break;
        }
        
        RINOK(ReadByte(f, &type));
    }
    
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
                        
                        /* Convert UTF-16LE to ASCII/UTF-8 (simplified) */
                        size_t nameLen = (pos - nameStart) / 2 - 1;
                        archive->files[i].Name = (char *)archive->alloc->Alloc(archive->alloc, nameLen + 1);
                        if (archive->files[i].Name)
                        {
                            size_t j;
                            for (j = 0; j < nameLen; j++)
                            {
                                UInt16 c = namesData[nameStart + j * 2] | 
                                          ((UInt16)namesData[nameStart + j * 2 + 1] << 8);
                                /* Simple ASCII conversion - non-ASCII chars become '?' */
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
    
    /* Read next header info */
    nextHeaderOffset = header[12] | ((UInt64)header[13] << 8) | 
                       ((UInt64)header[14] << 16) | ((UInt64)header[15] << 24) |
                       ((UInt64)header[16] << 32) | ((UInt64)header[17] << 40) |
                       ((UInt64)header[18] << 48) | ((UInt64)header[19] << 56);
    
    nextHeaderSize = header[20] | ((UInt64)header[21] << 8) | 
                     ((UInt64)header[22] << 16) | ((UInt64)header[23] << 24) |
                     ((UInt64)header[24] << 32) | ((UInt64)header[25] << 40) |
                     ((UInt64)header[26] << 48) | ((UInt64)header[27] << 56);
    
    nextHeaderCRC = header[28] | ((UInt32)header[29] << 8) | 
                    ((UInt32)header[30] << 16) | ((UInt32)header[31] << 24);
    
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
    /* Full extraction requires more complex stream handling
       For now, return unsupported */
    (void)archive;
    (void)fileIndex;
    (void)outBuf;
    (void)outSize;
    
    return SZ_ERROR_UNSUPPORTED;
}
