//epbf to probe at openat syscall
#include "vmlinux.h"
#include <bpf/bpf_helpers.h> //header for bpf helper functions
#include <bpf/bpf_core_read.h> //header for CO-RE helpers -> for safely reading kernel memory
#include <bpf/bpf_tracing.h> //header for bpf tracing helpers

#define EXT_LEN 8
#define FNAME_LEN 256

char LICENSE[] SEC("license") = "GPL"; //hint for verifier to allow GPL-only functions

//ebpf map to not trigger probe for tensorfetch daemon itself
struct {
    __uint(type, BPF_MAP_TYPE_HASH); 
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} pid_map SEC(".maps");


//ebpf ring buffer called events to send data to user space for footer read
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 1 << 24); //16MB
} events SEC(".maps");

struct event_t {
    __u32 pid;
    char filename[FNAME_LEN];
};

static __always_inline bool has_parquet_extension(const char *filename); //function prototype for fwd declaration

//tradeoff b/w kprobe and tracepoint: kprobe is more flexible, but tracepoint is more stable -> kprobe not always available, need exact function name; tracepoint always available,
// but need to know exact tracepoint name and is architecture agnostic
SEC("tracepoint/syscalls/sys_enter_openat") //attach to openat syscall entry
int trace_openat(struct trace_event_raw_sys_enter* ctx) {
    
    __u32 pid = (__u32)(bpf_get_current_pid_tgid() >> 32); //get current pid
    __u32 key = 0; 
    __u32 *exists = bpf_map_lookup_elem(&pid_map, &key); //check if pid exists in pid_map - key 0 is reserved for this purpose
    if (exists) { //if exists, return 0 to not trigger probe for tensorfetch daemon itself
        return 0;
    }

    const char *filename = (const char *)ctx->args[1]; //get filename from syscall arguments
    char name[64];
    int ret = bpf_probe_read_user_str(name, sizeof(name), filename);
    if (ret > 0) {
        bpf_printk("openat pid=%d name=%s\n", pid, name);
    }


    //check if filename has .parquet extension, only then send an event to user space
    if (!has_parquet_extension(filename)) {
        return 0;
    }

    //send filename to user space via ring buffer
    struct event_t *event = bpf_ringbuf_reserve(&events, sizeof(*event), 0);
    if (!event) {
        return 0; //failed to reserve space in ring buffer
    }
    event->pid = pid;
    bpf_probe_read_user_str(event->filename, sizeof(event->filename), filename); //read filename from user space
    bpf_ringbuf_submit(event, 0); //submit event to ring buffer   

    return 0;
}

static __always_inline bool has_parquet_extension(const char *filename) {
    char name[FNAME_LEN] = {};
    int len = bpf_probe_read_user_str(name, sizeof(name), filename);
    //include \0 in length check
    if(len <= EXT_LEN) {
        return false;
    }

    if(len > FNAME_LEN) {
        len = FNAME_LEN;
    }
    
    const char *ext = ".parquet";
    int start = FNAME_LEN - EXT_LEN - 1; //-1 for skipping \0
    //read last 8 bytes of filename to check for .parquet extension
    #pragma unroll
    for(int i = 0; i < EXT_LEN; i++) {
        if(name[start + i] != ext[i]) {
            return false;
        }
    }

    return true;
}