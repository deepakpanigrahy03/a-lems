/**
 * msr_read - Simple MSR reader helper
 * 
 * Usage: ./msr_read <cpu> <msr_address_hex>
 * Example: ./msr_read 0 0x621
 * 
 * Returns: MSR value as 64-bit decimal on success
 *          Error message on stderr, exit code 1 on failure
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <cpu> <msr_hex>\n", argv[0]);
        return 1;
    }
    
    // Parse CPU number
    int cpu = atoi(argv[1]);
    if (cpu < 0) {
        fprintf(stderr, "Invalid CPU number: %s\n", argv[1]);
        return 1;
    }
    
    // Parse MSR address (hex)
    char *endptr;
    uint64_t msr_addr = strtoull(argv[2], &endptr, 16);
    if (*endptr != '\0') {
        fprintf(stderr, "Invalid MSR address: %s (must be hex like 0x621)\n", argv[2]);
        return 1;
    }
    
    // Open MSR device
    char msr_path[256];
    snprintf(msr_path, sizeof(msr_path), "/dev/cpu/%d/msr", cpu);
    
    int fd = open(msr_path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", msr_path, strerror(errno));
        return 1;
    }
    
    // Read MSR value
    uint64_t value;
    ssize_t ret = pread(fd, &value, sizeof(value), msr_addr);
    if (ret != sizeof(value)) {
        fprintf(stderr, "Failed to read MSR 0x%llx: %s\n", 
                (unsigned long long)msr_addr, strerror(errno));
        close(fd);
        return 1;
    }
    
    close(fd);
    
    // Output as decimal (easier for Python to parse)
    printf("%llu\n", (unsigned long long)value);
    return 0;
}
