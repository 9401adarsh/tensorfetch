#include "vmlinux.h"
#include "event.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

#define MAX_FILENAME_LEN 256

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24); // 16 MB
} events SEC(".maps");

const volatile __u32 tensorfetch_pid = 0;

SEC("tracepoint/syscalls/sys_enter_openat")
int trace_openat(struct trace_event_raw_sys_enter *ctx) {
    
    struct event_t *e;
    const char *filename = (const char *)ctx->args[1];
    __u32 pid = bpf_get_current_pid_tgid() >> 32;

    // Filter out TensorFetch process if tensorfetch_pid is set, to avoid self-tracing
    if(tensorfetch_pid && pid == tensorfetch_pid)
        return 0;
    
    if (!filename)
        return 0;

    e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e)
        return 0;

    e->pid = pid;
    bpf_probe_read_user_str(e->filename, sizeof(e->filename), filename);
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    bpf_ringbuf_submit(e, 0);

    return 0;
}

char LICENSE[] SEC("license") = "GPL";