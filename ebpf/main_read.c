#include <stdio.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>

#include "trace_read.skel.h"

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

static void sig_int(int signo) {
    exiting = true;
}

// Callback invoked for every BPF ring buffer entry
static int handle_read_event(void *ctx, void *data, size_t data_sz)
{
    const struct read_event_t *e = data;

    printf("\n[READ EVENT]\n");
    printf(" pid:    %u\n", e->pid);
    printf(" comm:   %s\n", e->comm);
    printf(" fd:     %d\n", e->fd);
    printf(" offset: %llu\n", (unsigned long long)e->offset);
    printf(" len:    %llu\n", (unsigned long long)e->len);

    return 0;
}

int main()
{
    struct trace_read_bpf *skel = NULL;
    struct ring_buffer *rb = NULL;
    int err;

    signal(SIGINT, sig_int);

    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);

    // Load + verify BPF
    skel = trace_read_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "Failed to load trace_read BPF skeleton\n");
        return 1;
    }

    // Attach tracepoints
    err = trace_read_bpf__attach(skel);
    if (err) {
        fprintf(stderr, "Failed to attach trace_read BPF program\n");
        goto cleanup;
    }

    // Create ring buffer
    rb = ring_buffer__new(bpf_map__fd(skel->maps.events),
                          handle_read_event, NULL, NULL);

    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer for read events\n");
        goto cleanup;
    }

    printf("Listening for read/pread64 syscalls… Ctrl-C to exit.\n");

    while (!exiting) {
        err = ring_buffer__poll(rb, 50); // ms
        if (err == -EINTR)
            break;
    }

cleanup:
    ring_buffer__free(rb);
    trace_read_bpf__destroy(skel);

    return 0;
}