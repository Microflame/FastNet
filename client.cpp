#include <iostream>
#include <vector>
#include <chrono>
#include <random>

#include "common.hpp"

size_t KB = 1024;
size_t MB = 1024 * KB;

size_t BUFFER_SIZE = 128 * MB;

#if 1
size_t MIN_REQ_SIZE = 256;
#else
size_t MIN_REQ_SIZE = 500 * KB;
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

    std::vector<char> buffer(BUFFER_SIZE);
    InitProcess();

    int server_socked_fd = 0;
    FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));

    sockaddr_in server_socked_addr = {};
    server_socked_addr.sin_family = AF_INET;
    server_socked_addr.sin_port = htons(1339);
    server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(connect(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<size_t> uni(MIN_REQ_SIZE, MAX_REQ_SIZE);

    auto start_time = std::chrono::high_resolution_clock::now();
    auto next_report_time = start_time;
    size_t num_requests = 0;
    size_t bytes_sent = 0;
    while (IS_RUNNING) {
        size_t size = uni(rng);
        SockResult send_res = SendMessage_sendmsg(server_socked_fd, {buffer.data(), size});
        if (!send_res) break;
        SockResult recv_res = RecvMessage(server_socked_fd, {buffer.data(), buffer.size()});
        if (!recv_res) break;

        bytes_sent += send_res.size;
        num_requests += 1;

        auto cur_time = std::chrono::high_resolution_clock::now();
        if (cur_time >= next_report_time) {
            next_report_time += std::chrono::milliseconds(100);
            double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
            time /= 1'000'000;
            double rps = num_requests / time;
            double mbps = bytes_sent / time / MB;
            std::cout << '\r' << "Num reqs: " << num_requests << "\tRPS: " << rps << "\tThroughput: " << mbps << " MB/s";
            std::cout.flush();
        }
    }
    auto cur_time = std::chrono::high_resolution_clock::now();
    double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
    time /= 1'000'000;
    double rps = num_requests / time;
    double mbps = bytes_sent / time / MB;
    std::cout << '\r' << "Num reqs: " << num_requests << "\tRPS: " << rps << "\tThroughput: " << mbps << " MB/s" << '\n';

    close(server_socked_fd);
    return 0;
}