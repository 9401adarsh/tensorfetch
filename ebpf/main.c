#include <stdio.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include "trace_open.skel.h"

struct event_t {
    __u32 pid;
    char comm[16];
};

static volatile bool exiting = false;

static int handle_event(void *ctx, void *data, size_t data_sz) {
    const struct event_t *e = data;
    printf("PID: %-6u COMM: %s\n", e->pid, e->comm);
    return 0;
}

static void sig_int(int signo) {
    exiting = true;
}

int main() {
    struct trace_open_bpf *skel;
    struct ring_buffer *rb = NULL;
    int err;

    signal(SIGINT, sig_int);

    libbpf_set_strict_mode(LIBBPF_STRICT_ALL);

    skel = trace_open_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "Failed to open/load BPF skeleton\n");
        return 1;
    }

    err = trace_open_bpf__attach(skel);
    if (err) {
        fprintf(stderr, "Failed to attach BPF program: %d\n", err);
        goto cleanup;
    }

    rb = ring_buffer__new(bpf_map__fd(skel->maps.events),
                          handle_event, NULL, NULL);
    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer\n");
        goto cleanup;
    }

    printf("Listening for openat() syscalls... Ctrl-C to exit.\n");

    while (!exiting) {
        err = ring_buffer__poll(rb, 100 /* timeout ms */);
        if (err == -EINTR)
            break;
    }

cleanup:
    ring_buffer__free(rb);
    trace_open_bpf__destroy(skel);
    return 0;
}