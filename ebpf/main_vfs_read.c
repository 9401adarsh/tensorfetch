// main_vfs_read.c
#include <stdio.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>

#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#include "trace_vfs_read.skel.h"

#define MAX_FILENAME_LEN 256

// Must match your BPF layout exactly
struct read_event_t {
    __u32 pid;
    char filename[MAX_FILENAME_LEN]; // only if you added it; ignored if unused
    char comm[16];
    int fd;
    __u64 offset;
    __u64 len;
    int event_type; // should be 1
};

static volatile bool exiting = false;

static void sig_int(int signo)
{
    exiting = true;
}

static int handle_event(void *ctx, void *data, size_t data_sz)
{
    struct read_event_t *e = data;
    printf("PID: %-6u COMM: %-16s type=%d offset=%llu len=%llu\n",
           e->pid, e->comm, e->event_type,
           (unsigned long long)e->offset,
           (unsigned long long)e->len);

    return 0;
}

int main(void)
{
    struct trace_vfs_read_bpf *skel;
    struct ring_buffer *rb = NULL;
    int err;

    signal(SIGINT, sig_int);

    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);

    skel = trace_vfs_read_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "Failed to open/load trace_vfs_read BPF skeleton\n");
        return 1;
    }

    err = trace_vfs_read_bpf__attach(skel);
    if (err) {
        fprintf(stderr, "Failed to attach trace_vfs_read BPF program: %d\n", err);
        goto cleanup;
    }

    rb = ring_buffer__new(bpf_map__fd(skel->maps.events),
                          handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer\n");
        goto cleanup;
    }

    printf("Listening on kprobe/vfs_read... Ctrl-C to exit.\n");

    while (!exiting) {
        err = ring_buffer__poll(rb, 100 /* ms */);
        if (err == -EINTR)
            break;
    }

cleanup:
    ring_buffer__free(rb);
    trace_vfs_read_bpf__destroy(skel);
    return 0;
}