#pragma once

#include <string>
#include <mutex>
#include <cstdio>
#include <iostream>
#include <unistd.h>

// Logger for coordination results (synchronous disk flush)
class SynchronizedLogger {
public:
    SynchronizedLogger(const std::string& filename) {
        file_ = std::fopen(filename.c_str(), "a");
        if (!file_) {
            std::cerr << "Failed to open log: " << filename << std::endl;
        }
    }
    
    ~SynchronizedLogger() {
        if (file_) std::fclose(file_);
    }

    void log_sync(const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (file_) {
            std::fprintf(file_, "%s\n", message.c_str());
            std::fflush(file_);
            fsync(fileno(file_));
        }
    }

private:
    FILE* file_ = nullptr;
    std::mutex mutex_;
};
