#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "event.h"

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24);
} events SEC(".maps");

const volatile __u32 tensorfetch_pid = 0;

// sys_enter_pread64: args = (fd, buf, count, pos, ...)

SEC("tracepoint/syscalls/sys_enter_pread64")
int trace_enter_pread64(struct trace_event_raw_sys_enter *ctx)
{
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 pid      = pid_tgid >> 32;

    if (tensorfetch_pid && pid == tensorfetch_pid)
        return 0;

    int   fd     = (int)ctx->args[0];
    size_t count = (size_t)ctx->args[2];
    loff_t pos   = (loff_t)ctx->args[3];

    struct event_t *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e)
        return 0;

    e->pid        = pid;
    e->fd         = fd;
    e->offset     = pos;
    e->len        = count;
    e->event_type = 1;

    e->filename[0] = '\0';
    bpf_get_current_comm(&e->comm, sizeof(e->comm));
    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";