#include <span>
#include <string_view>
#include <stdexcept>
#include <cstring>
#include <functional>
#include <deque>
#include <vector>
#include <chrono>
#include <thread>
#include <iostream>

#include <stdio.h>
#include <netinet/ip.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <fcntl.h>

constexpr uint16_t PORT = 17000;
constexpr uint64_t RUN_TIME_S = 4;

size_t CHUNK_SIZE;
size_t NUM_CHUNKS_PER_WRITE;
double BYTES_TRANSMITTED = 0;

void PrintCError(const char* file, int line) {
    char ERROR_BUFFER[1024] = {};
    ERROR_BUFFER[0] = 0;
    snprintf(ERROR_BUFFER, sizeof(ERROR_BUFFER), "%s:%d", file, line);
    perror(ERROR_BUFFER);
}

in_addr_t MakeIpAddr(uint32_t b1, uint32_t b2, uint32_t b3, uint32_t b4) {
    return (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
}

#define FNET_EXIT_IF_ERROR(x)                                           \
    do {                                                                \
        if (int64_t(x) == -1) {                                         \
            PrintCError(__FILE__, __LINE__);                            \
            exit(EXIT_FAILURE);                                         \
        }                                                               \
    } while (0)

#define FNET_ASSERT(x)                                                  \
    do {                                                                \
        if (!(x)) {                                                     \
            throw std::runtime_error("(" #x ") was not fulfilled");     \
        }                                                               \
    } while (0)

void Server() {
    int server_socked_fd = -1;
    FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));

    int enable = 1;
    FNET_EXIT_IF_ERROR(setsockopt(server_socked_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)));

    sockaddr_in server_socked_addr = {};
    server_socked_addr.sin_family = AF_INET;
    server_socked_addr.sin_port = htons(PORT);
    server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

    FNET_EXIT_IF_ERROR(listen(server_socked_fd, 16));

    int client_fd = 0;
    FNET_EXIT_IF_ERROR(client_fd = accept(server_socked_fd, nullptr, nullptr));

    std::vector<char> buffer(CHUNK_SIZE);
    while (true) {
        int n = recv(client_fd, buffer.data(), buffer.size(), 0);
        if (n <= 0)
            return;
        BYTES_TRANSMITTED += n;
    }
}

void Client() {
    int server_socket_fd = -1;
    FNET_EXIT_IF_ERROR(server_socket_fd = socket(AF_INET, SOCK_STREAM, 0));

    sockaddr_in server_sockaddr = {};
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(PORT);
    server_sockaddr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(connect(server_socket_fd, (const struct sockaddr*) &server_sockaddr, sizeof(server_sockaddr)));

    auto now = std::chrono::high_resolution_clock::now();
    auto end_time = now + std::chrono::seconds(RUN_TIME_S);
    std::vector<char> buffer(NUM_CHUNKS_PER_WRITE * CHUNK_SIZE);
    while (now < end_time) {
        size_t sent = 0;
        while (sent < buffer.size()) {
            int n = 0;
            FNET_EXIT_IF_ERROR(n = send(server_socket_fd, buffer.data() + sent, buffer.size() - sent, 0));
            sent += n;
        }
        now = std::chrono::high_resolution_clock::now();
    }
    close(server_socket_fd);
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "wrong argc\n";
        return 1;
    }

    std::string chunk_size(argv[1]);
    CHUNK_SIZE = std::stoll(chunk_size);
    if (chunk_size.back() == 'K') {
        CHUNK_SIZE *= 1024;
    }

    NUM_CHUNKS_PER_WRITE = 8 * 1024 * 1024 / CHUNK_SIZE;

    std::thread th(Server);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Client();
    th.join();
    double MB_PER_S = BYTES_TRANSMITTED / 1024 / 1024 / RUN_TIME_S;
    std::cout << "Throughput: " << MB_PER_S << " MB/s\n";
    return 0;
}