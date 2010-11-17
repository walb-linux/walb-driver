#include <assert.h>
#include <stdio.h>

#include "bitmap.h"


void test(int size)
{
        int i;
        walb_bitmap_t *bmp;
        int ret;

        bmp = walb_bitmap_create(size);
        walb_bitmap_on(bmp, 0);
        walb_bitmap_on(bmp, 1);
        walb_bitmap_print(bmp);
        
        walb_bitmap_off(bmp, 1);
        walb_bitmap_print(bmp);
        
        walb_bitmap_clear(bmp);
        walb_bitmap_print(bmp);

        for (i = 0; i < size; i ++) {
                walb_bitmap_on(bmp, i);
        }
        walb_bitmap_print(bmp);
        ret = walb_bitmap_is_all_on(bmp);
        printf("is_all_on: %d\n", ret);
        assert(ret == 1);
        
        walb_bitmap_off(bmp, 2);
        walb_bitmap_print(bmp);
        ret = walb_bitmap_is_all_on(bmp);
        printf("is_all_on: %d\n", ret);
        assert(ret == 0);
        
        walb_bitmap_clear(bmp);
        ret = walb_bitmap_is_all_off(bmp);
        printf("is_all_on: %d\n", ret);
        assert(ret == 1);
        
        walb_bitmap_on(bmp, 2);
        ret = walb_bitmap_is_all_off(bmp);
        printf("is_all_on: %d\n", ret);
        assert(ret == 0);
        
        walb_bitmap_free(bmp);
}


int main()
{
        test(128);
        test(127);
        test(129);

	return 0;
}
