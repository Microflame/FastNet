#include <chrono>
#include <iostream>
#include <random>
#include <vector>

#include "fastnet.hpp"
#include "common.hpp"

size_t KB = 1024;
size_t MB = 1024 * KB;
size_t GB = 1024 * MB;

static bool IS_RUNNING = true;
static size_t QUEUE_SIZE = 10;
static constexpr size_t BUFFER_SIZE = 16 * 1024 * 1024;

struct RequestsDescr {
    size_t min_length = 0;
    size_t max_length = 0;
    size_t count = 0;
};

void OnSigInt(int) {
    if (!IS_RUNNING) {
        exit(1);
    }
    IS_RUNNING = false;
}

uint64_t marsaglia_xorshf96_next() {
    static thread_local uint64_t x = 123456789;
    static thread_local uint64_t y = 362436069;
    static thread_local uint64_t z = 521288629;

    x ^= x << 16;
    x ^= x >> 5;
    x ^= x << 1;

    uint64_t t = x;
    x = y;
    y = z;
    z = t ^ x ^ y;

    return z;
}

void FillRandomRequest(std::string& dest, size_t size) {
    dest.resize(size);
    uint64_t* data = (uint64_t*) dest.data();
    for (size_t i = 0; i < size / sizeof(uint64_t); ++i) {
        data[i] = marsaglia_xorshf96_next();
    }

    uint64_t last = marsaglia_xorshf96_next();
    for (size_t i = size / sizeof(uint64_t) * sizeof(uint64_t); i < size; ++i) {
        dest[i] = last;
        last >>= 8;
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, OnSigInt);
    FNET_ASSERT(argc == 3);
    int port = std::atoll(argv[1]);
    QUEUE_SIZE = std::atoll(argv[2]);

    std::vector<RequestsDescr> descrs = {
        {0, 32, 200'000},
        {1024, 2 * 1024, 100'000},
        {512 * 1024, 1024 * 1024, 5'000},
    };

    fnet::Client client("localhost", port);

    std::random_device rd;
    std::mt19937 rng(rd());

    std::string res;
    std::vector<char> buffer(16 * 1024 * 1024);
    std::vector<fnet::Request> reqs(QUEUE_SIZE);
    std::vector<std::string> reqs_contents(QUEUE_SIZE);

    for (const RequestsDescr& rd: descrs) {
        std::uniform_int_distribution<size_t> uni(rd.min_length, rd.max_length);
        std::cout << "Running " << rd.count << " tests of size " << rd.min_length << " to " << rd.max_length << "\n";

        double start_s = common::TimeS();
        size_t req_idx = 0;
        size_t bytes_sent = 0;

        {
        common::ProgressBar pb;
        for (size_t i = 0; i < rd.count; ++i) {
            size_t size = uni(rng);
            size_t wrapped_idx = req_idx % reqs.size();

            if (req_idx >= reqs.size()) {
                std::string res = client.GetResult(reqs[wrapped_idx]);
                FNET_ASSERT(res.size() == reqs_contents[wrapped_idx].size());
                FNET_ASSERT(memcmp(res.data(), reqs_contents[wrapped_idx].data(), res.size()) == 0);
                client.ReturnRequestBuffer(std::move(res));
            }
            FillRandomRequest(reqs_contents[wrapped_idx], size);
            reqs[wrapped_idx] = client.ScheduleRequest(reqs_contents[wrapped_idx]);
            bytes_sent += size;
            req_idx += 1;

            pb.Draw((i + 1) / float(rd.count));

            if (!IS_RUNNING) {
                IS_RUNNING = true;
                break;
            }
        }
        }

        double end_s = common::TimeS();
        double delta_s = end_s - start_s;
        double rps = rd.count / delta_s;
        std::cout << "\tSent: " << req_idx << " requests\n";
        std::cout << "\tRPS: " << rps << "\n";
        std::cout << "\tRound Trip Throughput: " << (bytes_sent / delta_s / GB) << " GB/s\n\n";
    }
    return 0;
}