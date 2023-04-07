#include <iostream>
#include <vector>

#include "common.hpp"

template <typename F> 
struct Defer {
    Defer(F f): fn(f) {}
    ~Defer() { fn(); }
    F fn;
};

int main(int argc, char* argv[]) {
    InitProcess();

    int server_socked_fd = 0;
    FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));
    int enable = 1;
    FNET_EXIT_IF_ERROR(setsockopt(server_socked_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)));

    sockaddr_in server_socked_addr = {};
    server_socked_addr.sin_family = AF_INET;
    server_socked_addr.sin_port = htons(1339);
    server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

    FNET_EXIT_IF_ERROR(listen(server_socked_fd, 16));


    std::vector<char> buffer(64 * 1024 * 1024);
    while (true) {
        int client_fd = 0;
        FNET_EXIT_IF_ERROR(client_fd = accept(server_socked_fd, nullptr, nullptr));
        Defer d([&](){ FNET_EXIT_IF_ERROR(close(client_fd)); });
        std::cerr << "Client connected.\n";

        while (true) {
            SockResult res = RecvMessage(client_fd, {buffer.data(), buffer.size()});
            if (!res) break;
            std::string response = "OK!\0";
            res = SendMessage_sendmsg(client_fd, {response.data(), response.size()});
            if (!res) break;
        }

        std::cerr << "Client disconnected.\n";
    }

    return 0;
}