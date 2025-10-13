#include <boost/asio.hpp>
#include <iostream>
#include <thread> 
#include <string> 
#include <vector>
#include <bpf/libbpf.h>
#include <unistd.h> // for getpid()
#include <atomic>
#include <signal.h>
#include "io_uring_worker.h"
#include "event.h"
#include "trace_open.skel.h"

static std::atomic<bool> running(true);
static io_uring_worker ioworker;

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

    if(filename.ends_with(".parquet")) {
        //std::cout << "Detected Parquet file access: " << filename << std::endl;
        std::cout << "PID: " << e->pid << " COMM: " << comm << " FILE: " << filename << std::endl;
        // Add custom logic for handling Parquet file access here
        ioworker.enqueue(filename);
    }   
    
    return 0;
}

int main() {

    // === Start io_uring worker ===
    if (!ioworker.start()) {
        std::cerr << "Failed to start io_uring worker" << std::endl;
        return 1;
    }

    struct trace_open_bpf *skeleton = trace_open_bpf__open_and_load();
    boost::asio::io_context io;
    
    if (!skeleton) {
        std::cerr << "Failed to open and load BPF skeleton..." << std::endl;
        return 1; 
    }

    if (trace_open_bpf__attach(skeleton) != 0) {
        std::cerr << "Failed to attach BPF program..." << std::endl;
        trace_open_bpf__destroy(skeleton);
        return 1; 
    }

    std::cout << "BPF program loaded and attached successfully..." << std::endl;

    //add ring buffer here..
    struct ring_buffer *rb = ring_buffer__new(bpf_map__fd(skeleton->maps.events), handle_event, nullptr, nullptr);
    
    if (!rb) {
        std::cerr << "Failed to create ring buffer..." << std::endl;
        trace_open_bpf__detach(skeleton);
        trace_open_bpf__destroy(skeleton);
        return 1;
    }

    std::cout << "Ring buffer created successfully..." << std::endl; 
    
    boost::asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](auto, int signal_number) {
        std::cout << "Received signal " << signal_number << ", shutting down..." << std::endl;
        running = false;
        io.stop();
    });

    std::thread poll_rb_thread([&](){
        std::cout<<"Starting ring buffer polling thread..."<<std::endl;
        while (running.load()) {
            int err = ring_buffer__poll(rb, -1);
            if (err < 0) {
                std::cerr << "Error polling ring buffer: " << err << std::endl;
                break;
            }
        }
    });

    
    //poll ring buffer here...
    std::cout<<"Starting io_context event loop for TensorFetch daemon..."<<std::endl;
    io.run(); 

    running = false;
    if(poll_rb_thread.joinable()) {
        poll_rb_thread.join();
    }
    ioworker.stop();
    std::cout << "TensorFetch daemon shutting down gracefully..." << std::endl;

    //destroy ring buffer here...
    ring_buffer__free(rb);
    trace_open_bpf__detach(skeleton);
    trace_open_bpf__destroy(skeleton);
    return 0;
}