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

    std::string s1 = "Request 1 data";
    std::string s2 = "Request 2 data";
    std::string s3 = "Request 3 data";
    std::string s4 = "Request 4 data";
    std::string s5 = "Request 5 data";
    std::string s6 = "Request 6 data";
    std::string s7 = "Request 7 data";
    std::string s8 = "Request 8 data";
    std::string s9 = "Request 9 data";
    std::string s10 = "Request 10 data";

    fnet::Request r1 = client.ScheduleRequest(s1);
    fnet::Request r2 = client.ScheduleRequest(s2);
    fnet::Request r3 = client.ScheduleRequest(s3);
    fnet::Request r4 = client.ScheduleRequest(s4);
    fnet::Request r5 = client.ScheduleRequest(s5);
    fnet::Request r6 = client.ScheduleRequest(s6);
    fnet::Request r7 = client.ScheduleRequest(s7);
    fnet::Request r8 = client.ScheduleRequest(s8);
    fnet::Request r9 = client.ScheduleRequest(s9);
    fnet::Request r10 = client.ScheduleRequest(s10);

    client.WaitAll();

    std::cout << "r1: " << client.GetResult(r1) << '\n'; 
    std::cout << "r2: " << client.GetResult(r2) << '\n'; 
    std::cout << "r3: " << client.GetResult(r3) << '\n'; 
    std::cout << "r4: " << client.GetResult(r4) << '\n'; 
    std::cout << "r5: " << client.GetResult(r5) << '\n'; 
    std::cout << "r6: " << client.GetResult(r6) << '\n'; 
    std::cout << "r7: " << client.GetResult(r7) << '\n'; 
    std::cout << "r8: " << client.GetResult(r8) << '\n'; 
    std::cout << "r9: " << client.GetResult(r9) << '\n'; 
    std::cout << "r10: " << client.GetResult(r10) << '\n'; 


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