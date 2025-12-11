#include "textflag.h"

// uint64 Rdtsc()
TEXT ·Rdtsc(SB), NOSPLIT, $0-8
    RDTSC                  // read timestamp counter
    SHLQ $32, DX            // shift upper 32 bits left
    ORQ AX, DX              // combine lower 32 bits
    MOVQ DX, ret+0(FP)      // store result in return slot
    RET
