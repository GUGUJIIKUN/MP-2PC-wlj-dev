#pragma once

#include <string>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <unistd.h>

// Logger for coordination results. log_sync() returns only after the caller's
// log record has joined a batch that was forced to disk.
class SynchronizedLogger {
public:
    SynchronizedLogger(const std::string& filename,
                       std::size_t group_flush_size = 64,
                       std::chrono::microseconds group_flush_timeout = std::chrono::microseconds(1000))
        : group_flush_size_(group_flush_size == 0 ? 1 : group_flush_size),
          group_flush_timeout_(group_flush_timeout.count() <= 0
                                   ? std::chrono::microseconds(1)
                                   : group_flush_timeout) {
        file_ = std::fopen(filename.c_str(), "a");
        if (!file_) {
            std::cerr << "Failed to open log: " << filename << std::endl;
        }
    }
    
    ~SynchronizedLogger() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_) {
            force_flush_locked();
            std::fclose(file_);
            file_ = nullptr;
        }
    }

    void log_sync(const std::string& message) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!file_) {
            return;
        }

        const uint64_t my_seq = ++last_enqueued_seq_;
        if (std::fprintf(file_, "%s\n", message.c_str()) < 0) {
            std::cerr << "Failed to write sync log: " << std::strerror(errno) << std::endl;
        }
        ++pending_records_;

        if (!leader_active_) {
            leader_active_ = true;
            const auto deadline = std::chrono::steady_clock::now() + group_flush_timeout_;

            while (pending_records_ < group_flush_size_) {
                if (batch_cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                    break;
                }
            }

            const uint64_t flush_to_seq = last_enqueued_seq_;
            force_flush_locked();
            durable_seq_ = flush_to_seq;
            pending_records_ = 0;
            leader_active_ = false;

            batch_cv_.notify_all();
            return;
        }

        if (pending_records_ >= group_flush_size_) {
            batch_cv_.notify_all();
        }

        batch_cv_.wait(lock, [this, my_seq]() {
            return durable_seq_ >= my_seq;
        });
    }

private:
    int fdatasync_locked(int fd) {
#if defined(__APPLE__)
        return fsync(fd);
#else
        return fdatasync(fd);
#endif
    }

    void force_flush_locked() {
        if (!file_) {
            return;
        }

        if (std::fflush(file_) != 0) {
            std::cerr << "Failed to flush sync log: " << std::strerror(errno) << std::endl;
        }

        int fd = fileno(file_);
        if (fd >= 0 && fdatasync_locked(fd) != 0) {
            std::cerr << "Failed to fdatasync sync log: " << std::strerror(errno) << std::endl;
        }
    }

    FILE* file_ = nullptr;
    std::mutex mutex_;
    std::condition_variable batch_cv_;
    std::size_t group_flush_size_;
    std::chrono::microseconds group_flush_timeout_;
    std::size_t pending_records_ = 0;
    uint64_t last_enqueued_seq_ = 0;
    uint64_t durable_seq_ = 0;
    bool leader_active_ = false;
};
