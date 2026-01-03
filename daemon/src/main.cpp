#include <boost/asio.hpp>
#include <iostream>
#include <thread> 
#include <string> 
#include <vector>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <unistd.h> // for getpid()
#include <atomic>
#include <signal.h>
#include <unordered_map>
#include "io_uring_worker.h"
#include "event.h"
#include "trace_open.skel.h"
#include  "trace_pread.skel.h"

static std::atomic<bool> running(true);
static io_uring_worker ioworker;
static int fd_to_filename_map_fd = -1;

int handle_event(void *ctx, void *data, size_t data_sz) {
    const struct event_t *e = reinterpret_cast<const struct event_t *>(data);
    if(!e) {
        std::cerr << "Received null event data" << std::endl;
        return -1;
    }
    if(e->pid == getpid()) {
        // Ignore events from the TensorFetch daemon itself
        return 0;
    }
    std::string filename(e->filename);
    std::string comm = e->comm;
    int fd = e->fd; 
    int event_type = e->event_type;

    if(event_type == 0) {
        // std::cout << "\n[PARQUET FILE OPEN EVENT] with fd: " << fd << std::endl;
        // std::cout << "Detected Parquet file access: " << filename << std::endl;
        // std::cout << "PID: " << e->pid << " COMM: " << comm << " FILE: " << filename << std::endl;

        uint64_t key = (static_cast<uint64_t>(e->pid) << 32) | static_cast<uint32_t>(fd);
        char fname_buf[MAX_FILENAME_LEN];
        ssize_t res = bpf_map_lookup_elem(fd_to_filename_map_fd, &key, &fname_buf);
        if (res < 0) {
            // Filename not found for this fd
            return 0;
        }
        ioworker.enqueue(filename, OPEN_EVENT, 0, 0, fd);
        return 0; 

    }   else if(event_type == 1) {
        // Handle pread64 events if needed
        // Currently just logging
        // std::cout << "[PREAD64 EVENT] PID: " << e->pid << " COMM: " << comm 
        //           << " FD: " << fd << " OFFSET: " << e->offset 
        //           << " LEN: " << e->len << std::endl; 
        uint64_t key = (static_cast<uint64_t>(e->pid) << 32) | static_cast<uint32_t>(fd);
        char fname_buf[MAX_FILENAME_LEN];
        ssize_t res = bpf_map_lookup_elem(fd_to_filename_map_fd, &key, &fname_buf);
        //ioworker.enqueue(std::string(fname_buf), PREAD64_EVENT, e->offset, e->len, fd);
        
    }
    return 0;
}

int main(int argc, char **argv) {

    prefetch_mode mode = SPECULATIVE;
    bool chunked_prefetch = false;
    bool sqpoll_enabled = false;
    bool adaptive_prefetch = false; 

    if(argc <= 1) {
        std::cerr << "Usage: " << argv[0] << " [exact|speculative] [chunked] [sqpoll] [adaptive]" << std::endl;
        return 1;
    }

    if(argc > 1) {
        std::string mode_arg = argv[1];
        if(mode_arg == "exact") {
            mode = EXACT;
        } else if(mode_arg == "speculative") {
            mode = SPECULATIVE;
        } else {
            std::cerr << "Unknown prefetch mode argument: " << mode_arg << ", defaulting to SPECULATIVE" << std::endl;
        }
    }

    if(argc > 2) {
        std::string chunked_arg = argv[2];
        if(chunked_arg == "chunked") {
            chunked_prefetch = true;
        }
    }

    if (argc > 3) {
       std::string sqpoll_arg = argv[3];
       if(sqpoll_arg == "sqpoll") {
           sqpoll_enabled = true;
       }
    }

    if (argc > 4) {
       std::string adaptive_arg = argv[4];
        if(adaptive_arg == "adaptive") {
            adaptive_prefetch = true;
        }
    }

    ioworker.set_prefetch_mode(mode);
    ioworker.set_chunked_prefetch(chunked_prefetch);
    ioworker.enable_sqpolling(sqpoll_enabled);
    ioworker.set_adaptive_prefetch(adaptive_prefetch);

    // === Start io_uring worker ===
    if (!ioworker.start()) {
        std::cerr << "Failed to start io_uring worker" << std::endl;
        return 1;
    }

    struct trace_open_bpf *skeleton = trace_open_bpf__open_and_load();
    if (!skeleton) {
        std::cerr << "Failed to open and load BPF skeleton for trace at open..." << std::endl;
        return 1; 
    }
    if (trace_open_bpf__attach(skeleton) != 0) {
        std::cerr << "Failed to attach BPF program for trace at open ..." << std::endl;
        trace_open_bpf__destroy(skeleton);
        return 1; 
    }
    std::cout << "BPF program loaded and attached successfully for trace at open ..." << std::endl;
    fd_to_filename_map_fd = bpf_map__fd(skeleton->maps.fd_to_filename);

    //add ring buffer here..
    struct ring_buffer *open_rb = ring_buffer__new(bpf_map__fd(skeleton->maps.events), handle_event, nullptr, nullptr);
    if (!open_rb) {
        std::cerr << "Failed to create ring buffer..." << std::endl;
        trace_open_bpf__detach(skeleton);
        trace_open_bpf__destroy(skeleton);
        return 1;
    }
    std::cout << "Ring buffer created for trace at open successfully..." << std::endl; 

    struct trace_pread_bpf *read_skel = trace_pread_bpf__open_and_load();
    if (!read_skel) {
        std::cerr << "Failed to open and load BPF skeleton for trace at pread..." << std::endl;
        return 1; 
    }
    if (trace_pread_bpf__attach(read_skel) != 0) {
        std::cerr << "Failed to attach BPF program for trace at pread ..." << std::endl;
        trace_pread_bpf__destroy(read_skel);
        return 1; 
    }
    std::cout << "BPF program loaded and attached successfully for trace at pread ..." << std::endl;
    //add ring buffer here..
    struct ring_buffer *read_rb = ring_buffer__new(bpf_map__fd(read_skel->maps.events), handle_event, nullptr, nullptr);
    if (!read_rb) {
        std::cerr << "Failed to create ring buffer for pread..." << std::endl;
        trace_pread_bpf__detach(read_skel);
        trace_pread_bpf__destroy(read_skel);
        return 1;
    }
    std::cout << "Ring buffer created for trace at pread successfully..." << std::endl;
    
    boost::asio::io_context io;
    boost::asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](auto, int signal_number) {
        std::cout << "Received signal " << signal_number << ", shutting down..." << std::endl;
        running = false;
        io.stop();
    });

    std::thread poll_open_rb_thread([&](){
        std::cout<<"Starting ring buffer polling thread..."<<std::endl;
        while (running.load()) {
            int err = ring_buffer__poll(open_rb, -1);
            if (err < 0) {
                std::cerr << "Error polling ring buffer: " << err << std::endl;
                break;
            }
        }
    });

    std::thread poll_read_rb_thread([&](){
        std::cout<<"Starting pread ring buffer polling thread..."<<std::endl;
        while (running.load()) {
            int err = ring_buffer__poll(read_rb, -1);
            if (err < 0) {
                std::cerr << "Error polling pread ring buffer: " << err << std::endl;
                break;
            }
        }
    });

    //poll ring buffer here...
    std::cout<<"Starting io_context event loop for TensorFetch daemon..."<<std::endl;
    io.run(); 

    running = false;
    if(poll_open_rb_thread.joinable()) {
        poll_open_rb_thread.join();
    }
    if(poll_read_rb_thread.joinable()) {
        poll_read_rb_thread.join();
    }
    ioworker.stop();
    std::cout << "TensorFetch daemon shutting down gracefully..." << std::endl;

    //destroy ring buffer here...
    ring_buffer__free(open_rb);
    trace_open_bpf__detach(skeleton);
    trace_open_bpf__destroy(skeleton);

    ring_buffer__free(read_rb);
    trace_pread_bpf__detach(read_skel);
    trace_pread_bpf__destroy(read_skel);

    return 0;
}