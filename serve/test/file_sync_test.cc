#include "sync_logger.h" 
#include <chrono>
#include <iostream>
#include <vector>
#include <thread>

void RunSyncTest(int num_threads, int writes_per_thread, const std::string& log_file_path) {
    SynchronizedLogger logger(log_file_path);
    std::vector<std::thread> threads;
    const std::string test_msg = "Transaction_12345_Status:COMMIT";
    std::vector<double> thread_latencies(num_threads, 0.0);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            double local_latency = 0.0;
            for (int j = 0; j < writes_per_thread; ++j) {
                auto call_start = std::chrono::high_resolution_clock::now();
                logger.log_sync(test_msg);
                auto call_end = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> ms = call_end - call_start;
                local_latency += ms.count();
            }
            thread_latencies[i] = local_latency;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    double total_call_latency = 0.0;
    for (double lat : thread_latencies) {
        total_call_latency += lat;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;
    
    int total_writes = num_threads * writes_per_thread;
    double total_seconds = diff.count();
    double avg_ms = (total_seconds * 1000.0) / total_writes;
    double avg_call_latency_ms = total_call_latency / total_writes;
    double throughput = total_writes / total_seconds;

    std::cout << "--- 测试结果 (" << num_threads << " 个线程) ---" << std::endl;
    std::cout << "总写入次数     : " << total_writes << std::endl;
    std::cout << "总耗时         : " << total_seconds << " 秒" << std::endl;
    std::cout << "整体平均耗时   : " << avg_ms << " ms/次" << std::endl;
    std::cout << "函数调用延迟   : " << avg_call_latency_ms << " ms/次 (log_sync 调用前后打点)" << std::endl;
    std::cout << "吞吐量 (TPS)   : " << throughput << " 次/秒\n" << std::endl;
}

int main(int argc, char* argv[]) {
    // 允许通过命令行参数传入日志路径
    std::string log_file_path = "test_sync_log.txt";
    if (argc > 1) {
        log_file_path = argv[1];
    }
    std::cout << "===== 开始测试，日志文件路径: " << log_file_path << " =====" << std::endl;

    // 1. 测试单线程情况 (主要看硬盘fsync的硬延迟)
    std::remove(log_file_path.c_str()); 
    RunSyncTest(1, 1000, log_file_path); 

    // 2. 测试 16/32 线程情况 (模拟你系统 run.cc 中的多线程)
    std::remove(log_file_path.c_str());
    RunSyncTest(32, 100, log_file_path); 

    return 0;
}
