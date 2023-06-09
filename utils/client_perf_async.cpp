#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <fstream>

#include "fastnet.hpp"
#include "common.hpp"

size_t KB = 1024;
size_t MB = 1024 * KB;

#if 0
size_t MIN_REQ_SIZE = 256;
#else
size_t MIN_REQ_SIZE = 512 * KB;
#endif

size_t MAX_REQ_SIZE = MIN_REQ_SIZE * 2;

bool IS_RUNNING = true;

void OnSigInt(int) {
    if (!IS_RUNNING) {
        exit(1);
    }
    IS_RUNNING = false;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, OnSigInt);
    FNET_ASSERT(argc == 2);
    int port = std::atoll(argv[1]);

    // common::Tracer::Get();

    fnet::Client client("localhost", port);

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<size_t> uni(MIN_REQ_SIZE, MAX_REQ_SIZE);

    std::string res;
    std::vector<char> buffer(16 * 1024 * 1024);
    auto start_time = std::chrono::high_resolution_clock::now();
    auto next_report_time = start_time;
    auto next_reset_time = start_time;
    size_t num_requests = 0;
    size_t bytes_sent = 0;

    std::vector<fnet::Request> reqs(3);
    size_t req_idx = 0;
    while (IS_RUNNING) {
        size_t size = uni(rng);
        size_t wrapped_idx = req_idx % reqs.size();

        if (req_idx >= reqs.size()) {
            auto begin = common::Tracer::Begin("Client/GetResults");
            std::string res = client.GetResult(reqs[wrapped_idx]);
            client.ReturnRequestBuffer(std::move(res));
            common::Tracer::End(begin);
        }
        reqs[wrapped_idx] = client.ScheduleRequest({buffer.data(), size});
        req_idx += 1;


        auto cur_time = std::chrono::high_resolution_clock::now();

        if (cur_time > next_reset_time) {
            next_reset_time = cur_time + std::chrono::seconds(5);
            next_report_time = cur_time + std::chrono::milliseconds(500);
            start_time = cur_time;
            bytes_sent = 0;
            num_requests = 0;
        }

        bytes_sent += size;
        num_requests += 1;

        if (cur_time >= next_report_time) {
            next_report_time = cur_time + std::chrono::milliseconds(500);
            double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
            time /= 1'000'000;
            double rps = num_requests / time;
            double mbps = bytes_sent / time / MB;
            std::cout << '\r' << "Num reqs: " << num_requests << " RPS: " << rps << " Throughput: " << mbps << " MB/s";
            std::cout.flush();
        }
    }
    auto cur_time = std::chrono::high_resolution_clock::now();
    double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
    time /= 1'000'000;
    double rps = num_requests / time;
    double mbps = bytes_sent / time / MB;
    std::cout << '\r' << "Num reqs: " << num_requests << " RPS: " << rps << " Throughput: " << mbps << " MB/s" << '\n';

    if (common::Tracer::INITIALIZED) {
        std::ofstream ofs("trace_client.json");
        FNET_ASSERT(ofs);
        ofs << common::Tracer::ToString();
    }

    return 0;
}