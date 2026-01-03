#include "vmlinux.h"
#include "event.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_FILENAME_LEN 256

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24);
} events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, __u32);
    __type(value, char[MAX_FILENAME_LEN]);
    __uint(max_entries, 1024);
} inflight_opens SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, __u64);
    __type(value, char[MAX_FILENAME_LEN]);
    __uint(max_entries, 1024);
} fd_to_filename SEC(".maps");

const volatile __u32 tensorfetch_pid = 0;

static __always_inline bool is_parquet(const char *s)
{
    const char suf[] = ".parquet";
    int len = 0;

    bpf_printk("is_parquet called\n");

    #pragma unroll
    for (int i = 0; i < MAX_FILENAME_LEN; i++) {
        char c = 0;
        if (bpf_probe_read_user(&c, sizeof(c), &s[i]) < 0)
            break;
        if (c == 0)
            break;
        len++;
    }
    if (len < 8)
        return false;

    #pragma unroll
    for (int i = 0; i < 8; i++) {
        char c = 0;
        bpf_probe_read_user(&c, sizeof(c), &s[len - 8 + i]);
        if (c != suf[i])
            return false;
    }
    return true;
}

// ===== ENTER OPENAT ======

SEC("tracepoint/syscalls/sys_enter_openat")
int trace_enter_openat(struct trace_event_raw_sys_enter *ctx)
{
    __u32 tgid = bpf_get_current_pid_tgid() >> 32;
    if (tensorfetch_pid && tgid == tensorfetch_pid)
        return 0;

    const char *filename = (const char *)ctx->args[1];

    if (!filename)
        return 0;

    if(!is_parquet(filename))
        return 0;

    char buf[MAX_FILENAME_LEN];
    if (bpf_probe_read_user_str(buf, sizeof(buf), filename) <= 0)
        return 0;

    /* store full filename into map under tgid */
    bpf_map_update_elem(&inflight_opens, &tgid, buf, BPF_ANY);
    return 0;
}


// ===== EXIT OPENAT ======

SEC("tracepoint/syscalls/sys_exit_openat")
int trace_exit_openat(struct trace_event_raw_sys_exit *ctx)
{
    __u32 tgid = bpf_get_current_pid_tgid() >> 32;
    if (tensorfetch_pid && tgid == tensorfetch_pid)
        return 0;

    long fd = ctx->ret;  // tracepoint field, safe to read directly
    if (fd < 0)
        return 0;

    /* lookup stored filename */
    char *fname = bpf_map_lookup_elem(&inflight_opens, &tgid);
    if (!fname)
        return 0;

    __u64 key = ((__u64)tgid << 32) | (__u32)fd;
    bpf_map_update_elem(&fd_to_filename, &key, fname, BPF_ANY);

    struct event_t *e = bpf_ringbuf_reserve(&events, sizeof(*e), 0);
    if (!e) {
        bpf_map_delete_elem(&inflight_opens, &tgid);
        return 0;
    }

    e->pid = tgid;
    e->fd = fd;
    e->event_type = 0;

    bpf_probe_read_kernel_str(e->filename, sizeof(e->filename), fname);
    bpf_get_current_comm(e->comm, sizeof(e->comm));

    bpf_ringbuf_submit(e, 0);

    /* cleanup */
    bpf_map_delete_elem(&inflight_opens, &tgid);
    return 0;
}

SEC("tracepoint/syscalls/sys_enter_close")
int enter_close(struct trace_event_raw_sys_enter *ctx)
{
    __u64 id = bpf_get_current_pid_tgid();
    __u32 fd = ctx->args[0];
    __u64 key = (id & 0xffffffff00000000ULL) | fd;

    bpf_map_delete_elem(&fd_to_filename, &key);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";