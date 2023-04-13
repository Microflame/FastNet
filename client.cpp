#include <iostream>
#include <vector>
#include <chrono>
#include <random>

#include "fastnet.hpp"

// size_t KB = 1024;
// size_t MB = 1024 * KB;

// #if 0
// size_t MIN_REQ_SIZE = 256;
// #else
// size_t MIN_REQ_SIZE = 512 * KB;
// #endif

// size_t MAX_REQ_SIZE = MIN_REQ_SIZE * 2;

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

    fnet::Client client("localhost", port);

    std::string req = "Test";
    std::string_view res = client.DoRequest(req);
    std::cout << res << '\n';


    // std::random_device rd;
    // std::mt19937 rng(rd());
    // std::uniform_int_distribution<size_t> uni(MIN_REQ_SIZE, MAX_REQ_SIZE);

    // auto start_time = std::chrono::high_resolution_clock::now();
    // auto next_report_time = start_time;
    // auto next_reset_time = start_time;
    // size_t num_requests = 0;
    // size_t bytes_sent = 0;
    // while (IS_RUNNING) {
    //     size_t size = uni(rng);
    //     SockResult send_res = SendMessage_sendmsg(server_socked_fd, {buffer.data(), size});
    //     if (!send_res) break;
    //     SockResult recv_res = RecvMessage_single(server_socked_fd, {buffer.data(), buffer.size()});
    //     if (!recv_res) break;

        // auto cur_time = std::chrono::high_resolution_clock::now();

        // if (cur_time > next_reset_time) {
        //     next_reset_time = cur_time + std::chrono::seconds(5);
        //     next_report_time = cur_time + std::chrono::milliseconds(500);
        //     start_time = cur_time;
        //     bytes_sent = 0;
        //     num_requests = 0;
        // }

        // bytes_sent += send_res.size;
        // num_requests += 1;

        // if (cur_time >= next_report_time) {
        //     next_report_time = cur_time + std::chrono::milliseconds(500);
        //     double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
        //     time /= 1'000'000;
        //     double rps = num_requests / time;
        //     double mbps = bytes_sent / time / MB;
        //     std::cout << '\r' << "Num reqs: " << num_requests << "\tRPS: " << rps << "\tThroughput: " << mbps << " MB/s";
        //     std::cout.flush();
        // }
    // }
    // auto cur_time = std::chrono::high_resolution_clock::now();
    // double time = std::chrono::duration_cast<std::chrono::microseconds>(cur_time - start_time).count();
    // time /= 1'000'000;
    // double rps = num_requests / time;
    // double mbps = bytes_sent / time / MB;
    // std::cout << '\r' << "Num reqs: " << num_requests << "\tRPS: " << rps << "\tThroughput: " << mbps << " MB/s" << '\n';

    return 0;
}