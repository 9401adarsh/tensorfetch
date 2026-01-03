#pragma once
#include <liburing.h>
#include <fstream>
#include <sstream>
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
#include <arrow/io/file.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#define FOOTER_READ_SIZE 8192  
#define BATCH_SIZE 8

struct Range { int64_t offset; int64_t length; };

enum event_type{
    OPEN_EVENT = 0,
    PREAD64_EVENT = 1
};

struct io_worker_job {
    std::string filename;
    event_type type; 
    int64_t offset;
    int64_t len; 
    int fd; 
};

enum prefetch_mode {
    EXACT,
    SPECULATIVE
};

struct io_ctx {
    int fd;
    void *buf;
    off_t size;
    off_t file_size;
    std::string filename;
};

class io_uring_worker {
    public: 
        io_uring_worker(): running_(false) {};
        ~io_uring_worker() { stop();}
        bool start(unsigned entries = 1024);
        void stop();
        void enqueue (const std::string &filepath, event_type type = OPEN_EVENT, int64_t offset = 0, int64_t len = 0, int fd = -1);

        void set_prefetch_mode(prefetch_mode mode) {
            pf_mode = mode;
        }
        void set_chunked_prefetch(bool enabled) {
            chunked_prefetch_enabled = enabled;
        }
        void enable_sqpolling(bool enable) {
            // Not implemented yet
            sqpoll_enabled = enable;
        }
        void set_adaptive_prefetch(bool enable) {
            adaptive_prefetch_enabled = enable;
        }

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
        // NOTE: read_parquet_footer is currently unused.
        // WARNING: fd ownership / close() logic needs review before use.
        
        void read_parquet_footer(std::vector<io_worker_job> &jobs);
        void prefetch_row_groups(const std::string &filepath);
        void exact_prefetch_row_groups(std::shared_ptr<parquet::FileMetaData> metadata, off_t file_size, std::vector<Range> &ranges);
        void speculative_prefetch_row_groups(std::shared_ptr<parquet::FileMetaData> metadata, off_t file_size, std::vector<Range> &ranges);
        void get_row_group_bounds_cache(const std::string &filepath, std::vector<Range> &ranges);
        void get_row_group_with_range(const std::string &filepath, int64_t offset, int64_t len);
        void loop();

        int64_t get_chunk_size_from_proc(); 
        static bool read_meminfo(long &mem_total_kb, long &mem_free_kb);

        //thread model - bookeeping and control variables
        io_uring ring_{}; 
        std::atomic<bool> running_;
        std::thread th_; 
        std::mutex m_; 
        std::condition_variable cv_; 
        std::queue<io_worker_job> job_q_; 

        //configuration options
        prefetch_mode pf_mode = SPECULATIVE;
        bool chunked_prefetch_enabled = false;
        bool adaptive_prefetch_enabled = false;
        bool sqpoll_enabled = false;

        //chunk size for adaptive prefetching
        int64_t default_chunk_size_ = 8LL * 1024 * 1024; // 8 MB
        int64_t max_chunk_size_ = 64LL * 1024 * 1024; // 64 MB
        int64_t min_chunk_size_ = 1LL * 1024 * 1024; // 1 MB

        std::unordered_map<std::string, std::map<int64_t, int64_t>> row_group_bounds_cache; // filename -> (offset -> length) // filename -> set of offsets being prefetched
};

bool io_uring_worker::start(unsigned entries) {
    if(running_) {
        std::cerr << "io_uring_worker already running..." << std::endl;
        return true;
    }
    struct io_uring_params p; 
    memset(&p, 0, sizeof(p));
    unsigned flags = 0;
    if(sqpoll_enabled) {
        flags |= IORING_SETUP_SQPOLL;
        p.sq_thread_cpu = 0; // Pin to CPU 0 for simplicity
        p.sq_thread_idle = 1000; // 2 seconds idle time
    }
    p.flags = flags;
    int ret = io_uring_queue_init_params(entries, &ring_, &p);
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

void io_uring_worker::enqueue (const std::string &filepath, event_type type, int64_t offset, int64_t len, int fd) {
    bool need_notify = false;
    {
        std::lock_guard<std::mutex> lock(m_);
        need_notify = job_q_.empty();
        job_q_.push(io_worker_job{filepath, event_type(type), offset, len, fd});
    }
    if(need_notify) {
        //std::cout << "[io_uring_worker] | New job enqueued on empty queue, notifying worker..." << std::endl;
        cv_.notify_one();
    }
}

void io_uring_worker::loop() {
    while(running_) {
        //std::cout << "[io_uring_worker] | Waiting for jobs..." << std::endl;
        std::vector<io_worker_job> jobs;
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
            std::unordered_map<std::string, bool> file_processed;
            for(const auto &job : jobs) {
                if(file_processed.find(job.filename) != file_processed.end()) {
                    continue; // Already processed this file in the batch
                }
                file_processed[job.filename] = true;
                //std::cout << "[io_uring_worker] | Processing job for file: " << job.filename << std::endl;
                auto evt_type = job.type;
                if(evt_type == OPEN_EVENT) {
                    // For OPEN_EVENT, we may want to read footer and prefetch
                    //std::cout << "[io_uring_worker] | Scheduling footer read for file: " << job.filename << std::endl;   
                    //read_parquet_footer({job}); // Read footer and prefetch
                    std::vector<Range> ranges;
                    //get_row_group_bounds_cache(job.filename, ranges);
                    prefetch_row_groups(job.filename);

                } else if(evt_type == PREAD64_EVENT) {
                    // For PREAD64_EVENT, we can implement logic if needed
                    // Currently, we just log or ignore
                    //std::cout << "[io_uring_worker] | Received PREAD64 event for file: " << job.filename 
                    //          << " Offset: " << job.offset << std::endl;
                    int64_t offset = job.offset;
                    int64_t len = job.len;
                    std::string filepath = job.filename;
                    auto it = row_group_bounds_cache.find(filepath);
                    if(it != row_group_bounds_cache.end()) {
                        auto &rg_map = it->second;
                        auto upper_bound = rg_map.upper_bound(offset);
                        if (upper_bound == rg_map.end()) {
                            // std::cout << "Already at last RG, no prefetch\n";
                            continue;
                        }

                        int64_t next_rg_start = upper_bound->first;
                        int64_t next_rg_len   = upper_bound->second;
                        int64_t next_rg_end   = next_rg_start + next_rg_len;

                        get_row_group_with_range(filepath, next_rg_start, next_rg_len);
                    }


                }
                //prefetch_row_groups(job.filename); // For testing, prefetch only the first job's file
            }
            //read_parquet_footer(jobs);
        }
    }
}

void io_uring_worker::read_parquet_footer(std::vector<io_worker_job> &jobs) {

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
    
        auto *udata = new io_ctx{fd, buf, read_size, file_size, job.filename};
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
            //process_footer_and_prefetch(udata, cqe->res);
            //std::cout << "[io_uring_worker] | Completed footer read for file: " << udata->filename << std::endl;
        }
        
        //std::cout<<"[io_uring_worker] | Footer read result for file: " << udata->filename << " res=" << cqe->res << std::endl;
        
        //std::cout<<"segfault test"<<std::endl;
        io_uring_cqe_seen(&ring_, cqe);
        std::free(udata->buf);
        close(udata->fd);
        delete udata;
    }

    return;
}

void io_uring_worker::prefetch_row_groups(const std::string &filepath) {
    // This function is now integrated into process_footer_and_prefetch.
    // Kept for potential future use.
    //std::cout << "[io_uring_worker] | Prefetching row groups for file: " << filepath << std::endl;

    // 1) Open with Arrow first
    auto st = arrow::io::ReadableFile::Open(filepath);
    if (!st.ok()) {
        std::cerr << "[io_uring_worker] | Arrow open failed for prefetch: " 
                << filepath << std::endl;
        return;
    }
    std::shared_ptr<arrow::io::ReadableFile> infile = *st;

    // 2) Get underlying fd (owned by Arrow, do NOT close(fd) yourself)
    int fd = infile->file_descriptor();
    if (fd < 0) {
        std::cerr << "[io_uring_worker] | Invalid file descriptor for: " 
                << filepath << std::endl;
        return;
    }

    // 3) Get file size (no ownership of fd here, just using it)
    off_t file_size;
    if (!fstat_size(fd, file_size)) {
        std::cerr << "[io_uring_worker] | Failed to get file size for prefetch: " 
                << filepath << std::endl;
        return;
    }

    // 4) Parquet reader + metadata
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::Open(infile);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    int num_row_groups = metadata->num_row_groups();
    if (num_row_groups <= 0) {
        std::cerr << "[io_uring_worker] | No row groups found for prefetch: " 
                << filepath << std::endl;
        return;
    }

    std::vector<Range> ranges;
    switch(pf_mode) {
        case EXACT:
            exact_prefetch_row_groups(metadata, file_size, ranges);
            break;
        case SPECULATIVE:
            speculative_prefetch_row_groups(metadata, file_size, ranges);
            break;
        default:
            std::cerr << "[io_uring_worker] | Unknown prefetch mode, skipping prefetch." << std::endl;
            return;
    }

    if(ranges.empty()) {
        //std::cout<<"[io_uring_worker] | No valid row group ranges for prefetch: " << filepath << std::endl;
        return;
    }

    //std::cout << "[io_uring_worker] | Prefetching " << ranges.size() << " row groups via io_uring..." << std::endl;

    std::vector<io_ctx*> rg_ctxs;
    int64_t adjusted_chunk_size = get_chunk_size_from_proc();
    
    for (const auto &r : ranges) {
        int64_t remaining = r.length;
        int64_t chunk_offset = r.offset; 
        while(remaining > 0){

            int64_t chunk_size = r.length; 
            if(chunked_prefetch_enabled) {
                chunk_size = std::min<int64_t>(adjusted_chunk_size, remaining);
            }
            if(remaining < chunk_size) {
                chunk_size = remaining;
            }; 

            void *buf = std::malloc(static_cast<size_t>(chunk_size));
            if(!buf) {
                std::cerr << "[io_uring_worker] | malloc failed for RG buffer" << std::endl;
                continue;   
            }

            io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if(!sqe) {
                std::cerr << "[io_uring_worker] | io_uring_get_sqe failed for RG" << std::endl;
                std::free(buf);
                continue;
            }

            auto *rg_ctx = new io_ctx{-1, buf, static_cast<off_t>(chunk_size), file_size, filepath};
            rg_ctxs.emplace_back(rg_ctx);
            io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(chunk_size), static_cast<off_t>(chunk_offset));
            //std::cout << "[chunk] offset=" << chunk_offset << " size=" << chunk_size << std::endl;
            io_uring_sqe_set_data(sqe, rg_ctx);
            remaining -= chunk_size;
            chunk_offset += chunk_size;
        }
    }

    if(rg_ctxs.empty()) {
        return;
    }

    int ret = io_uring_submit(&ring_);
    if(ret < 0) {
        std::cerr << "[io_uring_worker] | io_uring_submit for RG prefetch failed: " << ret << std::endl;
        for(auto *rg_ctx : rg_ctxs) {
            std::free(rg_ctx->buf);
            delete rg_ctx;
        }
        return;
    }

    for (size_t i = 0; i < rg_ctxs.size(); ++i) {
        io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(&ring_, &cqe);
        if(ret < 0) {
            std::cerr << "[io_uring_worker] | io_uring_wait_cqe (RG) failed: " << ret << std::endl;
            break;
        }
        auto *done_ctx = static_cast<io_ctx *>(io_uring_cqe_get_data(cqe));
        if(cqe->res < 0) {
            std::cerr << "[io_uring_worker] | RG prefetch failed for file: "
                      << done_ctx->filename << " res=" << cqe->res << std::endl;
        }
        else {
            // Successful prefetch; data is in done_ctx->buf (we don't need to inspect it).
            // This should have warmed the page cache.
        }
        io_uring_cqe_seen(&ring_, cqe);
        std::free(done_ctx->buf);
        delete done_ctx;
    }

    //std::cout << "[io_uring_worker] | Completed prefetching row groups for file: " << filepath << std::endl;
}

void io_uring_worker::exact_prefetch_row_groups(std::shared_ptr<parquet::FileMetaData> metadata, off_t file_size, std::vector<Range> &ranges) {
    int num_row_groups = metadata->num_row_groups();
    ranges.reserve(num_row_groups);
    for(int i = 0; i < num_row_groups; i++) {
        auto rg_start = std::numeric_limits<int64_t>::max();
        auto rg_end = int64_t{0};
        auto rg = metadata->RowGroup(i);
        for(int j = 0; j < rg->num_columns(); j++) {
            auto col_meta = rg->ColumnChunk(j);
            rg_start = std::min(rg_start, col_meta->data_page_offset());
            rg_end = std::max(rg_end, col_meta->data_page_offset() + col_meta->total_compressed_size());
        }
        int64_t rg_length = rg_end - rg_start;
        if(rg_length <= 0) continue;
        ranges.push_back({rg_start, rg_length});
        //std::cout << "[io_uring_worker] | Exact Prefetch - Row Group " << i << ": offset=" << rg_start << ", length=" << rg_length << std::endl;
        
    }
}

void io_uring_worker::speculative_prefetch_row_groups(std::shared_ptr<parquet::FileMetaData> metadata, off_t file_size, std::vector<Range> &ranges) {
    int num_row_groups = metadata->num_row_groups();
    ranges.reserve(num_row_groups);
    auto rg0 = metadata->RowGroup(0);
    int64_t first_rg_len = 0; 

    int64_t rg0_start = std::numeric_limits<int64_t>::max();
    int64_t rg0_end = 0;

    for (int j = 0; j < rg0->num_columns(); j++) {
        auto col_meta = rg0->ColumnChunk(j);
        rg0_start = std::min(rg0_start, col_meta->data_page_offset());
        rg0_end = std::max(rg0_end, col_meta->data_page_offset() + col_meta->total_compressed_size());
    }

    first_rg_len = rg0_end - rg0_start;
    if(first_rg_len <= 0) {
        return;
    }

    for (int i = 0; i < num_row_groups; i++) {
        int64_t rg_offset = rg0_start + i * first_rg_len; 
        int64_t rg_length = std::min(first_rg_len, file_size - rg_offset);
        if (rg_length <= 0) continue;

        ranges.push_back({rg_offset, rg_length});
        //std::cout << "[io_uring_worker] | Speculative Prefetch - Row Group " << i << ": offset=" << rg_offset << ", length=" << rg_length << std::endl;
    }
}

bool io_uring_worker::read_meminfo(long &mem_total_kb, long &mem_free_kb) {
    mem_total_kb = -1; 
    mem_free_kb = -1; 

    std::ifstream meminfo("/proc/meminfo");

    if (!meminfo.is_open()) {
        std::cerr << "[io_uring_worker] | Failed to open /proc/meminfo" << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(meminfo, line)) {
        std::istringstream iss(line);
        std::string key;
        long value;
        std::string unit;
        if(!(iss >> key >> value >> unit)) {
            continue; 
        }
        if (key == "MemTotal:") {
            mem_total_kb = value;
        } else if (key == "MemAvailable:") {
            mem_free_kb = value;
        }
        if (mem_total_kb != -1 && mem_free_kb != -1) {
            break; 
        }
    }

    if (mem_total_kb == -1 || mem_free_kb == -1) {
        std::cerr << "[io_uring_worker] | Failed to read MemTotal or MemFree from /proc/meminfo" << std::endl;
        return false;
    }

    return true; 
}

int64_t io_uring_worker::get_chunk_size_from_proc() {

    if(!chunked_prefetch_enabled) {
        return max_chunk_size_;
    };

    if(!adaptive_prefetch_enabled) {
        return default_chunk_size_;
    };

    long mem_total_kb, mem_free_kb;
    if(!read_meminfo(mem_total_kb, mem_free_kb)) {
        return default_chunk_size_;
    }

    double available_ratio = static_cast<double>(mem_free_kb) / static_cast<double>(mem_total_kb);

    double factor = 0.0;
    if (available_ratio >= 0.5) {
        factor = 2.0; 
    } else if (available_ratio > 0.25) {
        factor = 1.0; 
    } else if (available_ratio > 0.1) {
        factor = 0.5; 
    } else {
        factor = 0.25; 
    }

    int64_t adjusted_chunk_size = static_cast<int64_t>(default_chunk_size_ * factor);
    if (adjusted_chunk_size > max_chunk_size_) {
        adjusted_chunk_size = max_chunk_size_;
    } else if (adjusted_chunk_size < min_chunk_size_) {
        adjusted_chunk_size = min_chunk_size_;
    }

    //std::cout << "[io_uring_worker] | Adaptive chunk size adjusted to "  << adjusted_chunk_size / (1024 * 1024) << " MB based on memory availability." << "available ratio: " << available_ratio << std::endl;

    return adjusted_chunk_size;
}

void io_uring_worker::get_row_group_bounds_cache(const std::string &filepath, std::vector<Range> &ranges) {
    
    auto st = arrow::io::ReadableFile::Open(filepath);
    if (!st.ok()) {
        std::cerr << "[io_uring_worker] | Arrow open failed for prefetch: " 
                << filepath << std::endl;
        return;
    }
    std::shared_ptr<arrow::io::ReadableFile> infile = *st;

    // 2) Get underlying fd (owned by Arrow, do NOT close(fd) yourself)
    int fd = infile->file_descriptor();
    if (fd < 0) {
        std::cerr << "[io_uring_worker] | Invalid file descriptor for: " 
                << filepath << std::endl;
        return;
    }

    // 3) Get file size (no ownership of fd here, just using it)
    off_t file_size;
    if (!fstat_size(fd, file_size)) {
        std::cerr << "[io_uring_worker] | Failed to get file size for prefetch: " 
                << filepath << std::endl;
        return;
    }

    // 4) Parquet reader + metadata
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::Open(infile);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();

    int num_row_groups = metadata->num_row_groups();
    if (num_row_groups <= 0) {
        std::cerr << "[io_uring_worker] | No row groups found for prefetch: " 
                << filepath << std::endl;
        return;
    }

    exact_prefetch_row_groups(metadata, file_size, ranges);
    row_group_bounds_cache[filepath] = std::map<int64_t, int64_t>();
    for(const auto &r : ranges) {
        row_group_bounds_cache[filepath][r.offset] = r.length;
    }

    return; 
}

void io_uring_worker::get_row_group_with_range(const std::string &filepath, int64_t offset, int64_t len) {
    //std::cout << "[io_uring_worker] | get_row_group_with_range called for file: " << filepath 
    //          << " Offset: " << offset << " Len: " << len << std::endl;

    auto st = arrow::io::ReadableFile::Open(filepath);
    if (!st.ok()) {
        std::cerr << "[io_uring_worker] | Arrow open failed for prefetch: " 
                << filepath << std::endl;
        return;
    }
    std::shared_ptr<arrow::io::ReadableFile> infile = *st;

    int fd = infile->file_descriptor();
    if (fd < 0) {
        std::cerr << "[io_uring_worker] | Invalid file descriptor for: " 
                << filepath << std::endl;
        return;
    }

    // 3) Get file size (no ownership of fd here, just using it)
    off_t file_size;
    if (!fstat_size(fd, file_size)) {
        std::cerr << "[io_uring_worker] | Failed to get file size for prefetch: " 
                << filepath << std::endl;
        return;
    }

    std::vector<io_ctx*> rg_ctxs;
    int64_t adjusted_chunk_size = get_chunk_size_from_proc();

    int64_t remaining = len;
    int64_t chunk_offset = offset; 
    while(remaining > 0){

        int64_t chunk_size = len; 
        if(chunked_prefetch_enabled) {
            chunk_size = std::min<int64_t>(adjusted_chunk_size, remaining);
        }
        if(remaining < chunk_size) {
            chunk_size = remaining;
        }; 

        void *buf = std::malloc(static_cast<size_t>(chunk_size));
        if(!buf) {
            std::cerr << "[io_uring_worker] | malloc failed for RG buffer" << std::endl;
            continue;   
        }

        io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if(!sqe) {
            std::cerr << "[io_uring_worker] | io_uring_get_sqe failed for RG" << std::endl;
            std::free(buf);
            continue;
        }

        auto *rg_ctx = new io_ctx{-1, buf, static_cast<off_t>(chunk_size), file_size, filepath};
        rg_ctxs.emplace_back(rg_ctx);
        io_uring_prep_read(sqe, fd, buf, static_cast<unsigned>(chunk_size), static_cast<off_t>(chunk_offset));
        std::cout << "[chunk] offset=" << chunk_offset << " size=" << chunk_size << std::endl;
        io_uring_sqe_set_data(sqe, rg_ctx);
        remaining -= chunk_size;
        chunk_offset += chunk_size;
    }

    if(rg_ctxs.empty()) {
        return;
    }

    int ret = io_uring_submit(&ring_);
    if(ret < 0) {
        std::cerr << "[io_uring_worker] | io_uring_submit for RG prefetch failed: " << ret << std::endl;
        for(auto *rg_ctx : rg_ctxs) {
            std::free(rg_ctx->buf);
            delete rg_ctx;
        }
        return;
    }

    for (size_t i = 0; i < rg_ctxs.size(); ++i) {
        io_uring_cqe *cqe;
        ret = io_uring_wait_cqe(&ring_, &cqe);
        if(ret < 0) {
            std::cerr << "[io_uring_worker] | io_uring_wait_cqe (RG) failed: " << ret << std::endl;
            break;
        }
        auto *done_ctx = static_cast<io_ctx *>(io_uring_cqe_get_data(cqe));
        if(cqe->res < 0) {
            std::cerr << "[io_uring_worker] | RG prefetch failed for file: "
                      << done_ctx->filename << " res=" << cqe->res << std::endl;
        }
        else {
            // Successful prefetch; data is in done_ctx->buf (we don't need to inspect it).
            // This should have warmed the page cache.
        }
        io_uring_cqe_seen(&ring_, cqe);
        std::free(done_ctx->buf);
        delete done_ctx;
    }
}