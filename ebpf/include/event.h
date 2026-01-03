#ifndef EVENT_H
#define EVENT_H
#define MAX_FILENAME_LEN 256

struct event_t
{
    __UINT32_TYPE__ pid;
    char filename[MAX_FILENAME_LEN];
    char comm[16];
    int fd; 
    __UINT64_TYPE__ offset; 
    __UINT64_TYPE__ len;
    int event_type; // 0 for open, 1 for pread64
};

#endif
