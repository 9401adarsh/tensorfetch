// trace_vfs_read.bpf.c
#include "vmlinux.h"
#include "event.h"

#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24); // 16MB
} events SEC(".maps");

// Optional: filter out TensorFetch itself, like in trace_open
const volatile __u32 tensorfetch_pid = 0;

SEC("kprobe/vfs_read")
int trace_vfs_read(struct pt_regs *ctx)
{
    __u32 pid = bpf_get_current_pid_tgid() >> 32;

    // Don’t trace ourselves if pid is set
    if (tensorfetch_pid && pid == tensorfetch_pid)
        return 0;

    struct file *file = (struct file *)PT_REGS_PARM1(ctx);
    char *buf = (char *)PT_REGS_PARM2(ctx);
    size_t count = (size_t)PT_REGS_PARM3(ctx);
    loff_t *posp = (loff_t *)PT_REGS_PARM4(ctx);

    loff_t pos = 0;
    if (posp) {
        // Read the current file position before the read
        bpf_core_read(&pos, sizeof(pos), posp);
    }

    struct event_t *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e)
        return 0;

    // Zero it just in case (not strictly required, but nice during dev)
    __builtin_memset(e, 0, sizeof(*e));

    e->pid        = pid;
    e->fd         = -1;           // we don't have an fd here, only struct file*
    e->offset     = (unsigned long long)pos;
    e->len        = (unsigned long long)count;
    e->event_type = 1;

    // No filename here (we're at vfs_read, not open), leave e->filename empty
    bpf_get_current_comm(&e->comm, sizeof(e->comm));

    bpf_ringbuf_submit(e, 0);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";