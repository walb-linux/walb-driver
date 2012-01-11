/**
 * Test of XXX_u64bits() functions.
 */
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "walb/u64bits.h"
#include "random.h"


void print_bit_ary(int *bit_ary)
{
        int i;
        for (i = 0; i < 64; i ++) {
                printf("%d", bit_ary[i]);
        }
        printf("\n");
}

/**
 * Print u64 bits for debug.
 */
void print_u64bits(u64 *bits)
{
        int i;

        printf("%0"PRIx64"\n", *bits);

        for (i = 0; i < 64; i ++) {
                printf("%d", (test_u64bits(i, bits) == 0 ? 0 : 1));
        }
        printf("\n");
}


bool is_the_same(int *bit_ary, u64 *bits)
{
        int i;
        for (i = 0; i < 64; i ++) {
                if (test_u64bits(i, bits)) {
                        if (bit_ary[i] == 0) {
                                goto error0;
                        }
                }
        }
        return true;

error0:
        printf("error\n");
        print_bit_ary(bit_ary);
        print_u64bits(bits);
        return false;
}

int main()
{
        int i;
        int bit_ary[64];
        u64 bits;

        init_random();

        /* Initialize */
        for (i = 0; i < 63; i ++) {
                bit_ary[i] = get_random(2);
        }
        for (i = 0; i < 63; i ++) {
                if (bit_ary[i]) {
                        set_u64bits(i, &bits);
                } else {
                        clear_u64bits(i, &bits);
                }
        }
        ASSERT(is_the_same(bit_ary, &bits));
        
        /* Randomly set and check. */
        for (i = 0; i < 100000; i ++) {

                int j = get_random(64);
                
                if (get_random(2)) {
                        bit_ary[j] = 1;
                        set_u64bits(j, &bits);
                } else {
                        bit_ary[j] = 0;
                        clear_u64bits(j, &bits);
                }
                
                ASSERT(is_the_same(bit_ary, &bits));
        }        

        return 0;
}
