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

// Logger for coordination results. log_sync() returns after the caller's
// record joins a batch that has been forced to disk.
class SynchronizedLogger {
public:
    SynchronizedLogger(
        const std::string& filename,
        std::size_t batch_size = 64,
        std::chrono::microseconds batch_timeout = std::chrono::microseconds(1000)
    ) : batch_size_(batch_size), batch_timeout_(batch_timeout) {
        file_ = std::fopen(filename.c_str(), "a");
        if (!file_) {
            std::cerr << "Failed to open log: " << filename << std::endl;
        }
    }
    
    ~SynchronizedLogger() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (file_) {
            flush_locked();
            std::fclose(file_);
            file_ = nullptr;
        }
    }

    void log_sync(const std::string& message) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!file_) {
            return;
        }

        const uint64_t my_ticket = ++next_ticket_;
        if (std::fprintf(file_, "%s\n", message.c_str()) < 0) {
            std::cerr << "Failed to write sync log: " << std::strerror(errno) << std::endl;
        }

        if (flushing_) {
            cv_.notify_one();
            cv_.wait(lock, [&] { return durable_ticket_ >= my_ticket; });
            return;
        }

        flushing_ = true;
        uint64_t batch_target = next_ticket_;
        const auto deadline = std::chrono::steady_clock::now() + batch_timeout_;
        while (batch_target - durable_ticket_ < batch_size_) {
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                break;
            }
            batch_target = next_ticket_;
        }

        flush_locked();
        durable_ticket_ = batch_target;
        flushing_ = false;
        cv_.notify_all();
    }

private:
    void flush_locked() {
        if (!file_) {
            return;
        }
        if (std::fflush(file_) != 0) {
            std::cerr << "Failed to flush sync log: " << std::strerror(errno) << std::endl;
            return;
        }
#ifdef __linux__
        if (fdatasync(fileno(file_)) != 0) {
            std::cerr << "Failed to fdatasync sync log: " << std::strerror(errno) << std::endl;
        }
#else
        if (fsync(fileno(file_)) != 0) {
            std::cerr << "Failed to fsync sync log: " << std::strerror(errno) << std::endl;
        }
#endif
    }

    FILE* file_ = nullptr;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t batch_size_ = 64;
    std::chrono::microseconds batch_timeout_{1000};
    uint64_t next_ticket_ = 0;
    uint64_t durable_ticket_ = 0;
    bool flushing_ = false;
};
