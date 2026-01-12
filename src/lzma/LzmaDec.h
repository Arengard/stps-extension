/* LzmaDec.h -- LZMA Decoder
   Adapted from LZMA SDK by Igor Pavlov : Public domain
   Simplified for 7z encoded header support */

#ifndef __LZMA_DEC_H
#define __LZMA_DEC_H

#include "7zTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

/* _LZMA_PROB32 can increase speed but doubles memory usage */
typedef UInt16 CLzmaProb;

/* LZMA Properties */
#define LZMA_PROPS_SIZE 5

typedef struct _CLzmaProps
{
    Byte lc;
    Byte lp;
    Byte pb;
    Byte _pad_;
    UInt32 dicSize;
} CLzmaProps;

/* Decode properties from 5-byte array
   Returns: SZ_OK or SZ_ERROR_UNSUPPORTED */
SRes LzmaProps_Decode(CLzmaProps *p, const Byte *data, unsigned size);

/* LZMA_REQUIRED_INPUT_MAX = number of required input bytes for worst case */
#define LZMA_REQUIRED_INPUT_MAX 20

typedef struct
{
    CLzmaProps prop;
    CLzmaProb *probs;
    CLzmaProb *probs_1664;
    Byte *dic;
    size_t dicBufSize;
    size_t dicPos;
    const Byte *buf;
    UInt32 range;
    UInt32 code;
    UInt32 processedPos;
    UInt32 checkDicSize;
    UInt32 reps[4];
    UInt32 state;
    UInt32 remainLen;
    UInt32 numProbs;
    unsigned tempBufSize;
    Byte tempBuf[LZMA_REQUIRED_INPUT_MAX];
} CLzmaDec;

#define LzmaDec_Construct(p) { (p)->dic = NULL; (p)->probs = NULL; }

void LzmaDec_Init(CLzmaDec *p);

typedef enum
{
    LZMA_FINISH_ANY,   /* finish at any point */
    LZMA_FINISH_END    /* block must be finished at the end */
} ELzmaFinishMode;

typedef enum
{
    LZMA_STATUS_NOT_SPECIFIED,
    LZMA_STATUS_FINISHED_WITH_MARK,
    LZMA_STATUS_NOT_FINISHED,
    LZMA_STATUS_NEEDS_MORE_INPUT,
    LZMA_STATUS_MAYBE_FINISHED_WITHOUT_MARK
} ELzmaStatus;

/* Allocate and free decoder */
SRes LzmaDec_AllocateProbs(CLzmaDec *p, const Byte *props, unsigned propsSize, ISzAlloc *alloc);
void LzmaDec_FreeProbs(CLzmaDec *p, ISzAlloc *alloc);

SRes LzmaDec_Allocate(CLzmaDec *p, const Byte *props, unsigned propsSize, ISzAlloc *alloc);
void LzmaDec_Free(CLzmaDec *p, ISzAlloc *alloc);

/* Decode to dictionary */
SRes LzmaDec_DecodeToDic(CLzmaDec *p, size_t dicLimit,
    const Byte *src, size_t *srcLen, ELzmaFinishMode finishMode, ELzmaStatus *status);

/* Decode to buffer */
SRes LzmaDec_DecodeToBuf(CLzmaDec *p, Byte *dest, size_t *destLen,
    const Byte *src, size_t *srcLen, ELzmaFinishMode finishMode, ELzmaStatus *status);

/* One-call decode function */
SRes LzmaDecode(Byte *dest, size_t *destLen, const Byte *src, size_t *srcLen,
    const Byte *propData, unsigned propSize, ELzmaFinishMode finishMode,
    ELzmaStatus *status, ISzAlloc *alloc);

#ifdef __cplusplus
}
#endif

#endif /* __LZMA_DEC_H */
