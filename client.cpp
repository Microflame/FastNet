#include <iostream>

#include "common.hpp"


int main(int argc, char* argv[]) {
    char BUFFER[2048];
    std::span<char> buffer(BUFFER, sizeof(BUFFER));
    InitProcess();

    int server_socked_fd = 0;
    FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));

    sockaddr_in server_socked_addr = {};
    server_socked_addr.sin_family = AF_INET;
    server_socked_addr.sin_port = htons(1339);
    server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(connect(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

    std::string line;
    while (std::getline(std::cin, line)) {
        std::span<char> message(line.data(), line.size());
        SockResult res = SendMessage_sendmsg(server_socked_fd, message);
        if (!res) break;
        res = RecvMessage(server_socked_fd, buffer);
        if (!res) break;
        std::cout << ">>> " << std::string_view{BUFFER + sizeof(Header), res.size - sizeof(Header)} << '\n';
    }

    close(server_socked_fd);
    return 0;
}