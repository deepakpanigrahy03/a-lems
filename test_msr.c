#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
int main() {
    int fd = open("/dev/cpu/0/msr", O_RDONLY);
    if (fd < 0) {
        perror("open failed");
        return 1;
    }
    printf("✅ Success! fd=%d\n", fd);
    close(fd);
    return 0;
}
