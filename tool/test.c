#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>

int main()
{
	bool a;
	
	printf("%d\n", getpagesize());
	printf("%d %d %d\n", true, false, a);
	
	return 0;
}
