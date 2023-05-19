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

    fnet::RequestFuturePtr r1 = client.ScheduleRequest(s1);
    fnet::RequestFuturePtr r2 = client.ScheduleRequest(s2);
    fnet::RequestFuturePtr r3 = client.ScheduleRequest(s3);
    fnet::RequestFuturePtr r4 = client.ScheduleRequest(s4);
    fnet::RequestFuturePtr r5 = client.ScheduleRequest(s5);
    fnet::RequestFuturePtr r6 = client.ScheduleRequest(s6);
    fnet::RequestFuturePtr r7 = client.ScheduleRequest(s7);
    fnet::RequestFuturePtr r8 = client.ScheduleRequest(s8);
    fnet::RequestFuturePtr r9 = client.ScheduleRequest(s9);
    fnet::RequestFuturePtr r10 = client.ScheduleRequest(s10);

    r10->Wait();

    std::cout << "r1: " << r1->GetResult() << '\n'; 
    std::cout << "r2: " << r2->GetResult() << '\n'; 
    std::cout << "r3: " << r3->GetResult() << '\n'; 
    std::cout << "r4: " << r4->GetResult() << '\n'; 
    std::cout << "r5: " << r5->GetResult() << '\n'; 
    std::cout << "r6: " << r6->GetResult() << '\n'; 
    std::cout << "r7: " << r7->GetResult() << '\n'; 
    std::cout << "r8: " << r8->GetResult() << '\n'; 
    std::cout << "r9: " << r9->GetResult() << '\n'; 
    std::cout << "r10: " << r10->GetResult() << '\n'; 


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