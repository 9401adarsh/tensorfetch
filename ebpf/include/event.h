#ifndef EVENT_H
#define EVENT_H
#define MAX_FILENAME_LEN 256

struct event_t
{
    __UINT32_TYPE__ pid;
    char filename[MAX_FILENAME_LEN];
    char comm[16];
};

#endif
