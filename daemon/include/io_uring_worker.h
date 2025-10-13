#pragma once
#include <liburing.h>
#include <string> 
#include <queue> 
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

#define FOOTER_READ_SIZE 8192  
#define BATCH_SIZE 8

struct footer_job {
    std::string filename;
};

struct io_ctx {
    int fd;
    void *buf;
    off_t size;
    std::string filename;
};

class io_uring_worker {
    public: 
        io_uring_worker(): running_(false) {};
        ~io_uring_worker() { stop();}
        bool start(unsigned entries = 1024);
        void stop();
        void enqueue (const std::string &filepath);
    private: 
        static bool fstat_size(int fd, off_t &size) {
            struct stat st;
            if(fstat(fd, &st) < 0) {
                std::cerr << "fstat failed: "<<std::endl;
                return false;
            }
            size = st.st_size;
            return true;
        }
        int do_read(int fd, off_t offset, size_t size, void *buf);
        void read_parquet_footer(std::vector<footer_job> &jobs);
        void loop();

        //variables
        io_uring ring_{}; 
        std::atomic<bool> running_;
        std::thread th_; 
        std::mutex m_; 
        std::condition_variable cv_; 
        std::queue<footer_job> job_q_; 
};

bool io_uring_worker::start(unsigned entries) {
    if(running_) {
        std::cerr << "io_uring_worker already running..." << std::endl;
        return true;
    }
    int ret = io_uring_queue_init(entries, &ring_, 0);
    if(ret < 0) {
        std::cerr << "io_uring_queue_init failed: " << ret << std::endl;
        return false;
    }
    running_ = true;
    th_ = std::thread(&io_uring_worker::loop, this);
    return true;
}

void io_uring_worker::stop() {
    //ensures only one thread calls stop
    if(!running_.exchange(false)) {
        return;
    }
    cv_.notify_all();
    if (th_.joinable()) {
        th_.join();
    }
    io_uring_queue_exit(&ring_);
}

void io_uring_worker::enqueue (const std::string &filepath) {
    bool need_notify = false;
    {
        std::lock_guard<std::mutex> lock(m_);
        need_notify = job_q_.empty();
        job_q_.push(footer_job{filepath});
    }
    if(need_notify) {
        //std::cout << "[io_uring_worker] | New job enqueued on empty queue, notifying worker..." << std::endl;
        cv_.notify_one();
    }
}

void io_uring_worker::loop() {
    while(running_) {
        //std::cout << "[io_uring_worker] | Waiting for jobs..." << std::endl;
        std::vector<footer_job> jobs;
        {
            std::unique_lock<std::mutex> lock(m_);
            cv_.wait(lock, [this] { return !job_q_.empty() || !running_; });
            if(!running_ && job_q_.empty()) {
                break;
            }            
            while(!job_q_.empty() && jobs.size() < BATCH_SIZE) {
                jobs.push_back(job_q_.front());
                job_q_.pop();
            }
            lock.unlock();
        }
        if(!jobs.empty()) {
            read_parquet_footer(jobs);
        }
    }
}

void io_uring_worker::read_parquet_footer(std::vector<footer_job> &jobs) {

    std::vector<io_ctx*> contexts;
    for(const auto &job : jobs) {
        std::string filepath = job.filename;
        //std::cout << "Scheduling read for Parquet footer of file: " << filepath << std::endl;   
        int fd = open(filepath.c_str(), O_RDONLY);
        off_t file_size;

        if(fd < 0) {
            std::cerr << "[io_uring_worker] | Failed to open file: " << filepath << std::endl;
            continue;
        }

        if(!fstat_size(fd, file_size)){
            std::cerr << "[io_uring_worker] | Failed to get file size for: " << filepath << std::endl;
            close(fd);
            continue;
        }

        off_t read_size = std::min<off_t>(FOOTER_READ_SIZE, file_size);
        off_t offset = file_size - read_size;

        void *buf = std::malloc(read_size);
        if(!buf) {
            std::cerr << "[io_uring_worker] | Failed to malloc for buf" << std::endl;
            close(fd);
            return;
        }

        io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if(!sqe) {
            std::cerr << "[io_uring_worker] | io_uring_get_sqe failed" << std::endl;
            free(buf);
            close(fd);
            continue;
        }
    
        auto *udata = new io_ctx{fd, buf, read_size, job.filename};
        contexts.emplace_back(udata);
        io_uring_prep_read(sqe, fd, buf, read_size, offset);
        io_uring_sqe_set_data(sqe, udata);
    }

    //std::cout << "SQ ring space left: " << io_uring_sq_space_left(&ring_) << std::endl;
    int ret = io_uring_submit(&ring_);
    //std::cout << "Submitted " << ret << " read requests to io_uring" << std::endl;
    if(ret < 0) {
        std::cerr << "[io_uring_worker] | io_uring_submit failed: " << ret << std::endl;
        for(auto *ctx : contexts) {
            std::free(ctx->buf);
            close(ctx->fd);
            delete ctx;
        }
    }

    for (size_t i = 0; i < jobs.size(); ++i) {
        io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(&ring_, &cqe);
        if(ret < 0) {
            std::cerr << "[io_uring_worker] | io_uring_wait_cqe failed: " << ret << std::endl;
            continue;
        }
        auto *udata = static_cast<io_ctx *>(io_uring_cqe_get_data(cqe));
        if(cqe->res < 0) {
            std::cerr << "[io_uring_worker] | Read failed for: " << udata->filename << std::endl;
        } else {
            // Process the footer data in buf of size cqe->res
            //std::cout << "Read " << cqe->res << " bytes for file: " << udata->filename << std::endl;
            // Add logic to parse and handle Parquet footer here
        }
        
        io_uring_cqe_seen(&ring_, cqe);
        std::free(udata->buf);
        close(udata->fd);
        delete udata;
    }

    return;
}



