#include <iostream>
#include <vector>

#include "fastnet.hpp"

constexpr size_t MAX_NUM_CLIENTS = 128;
constexpr size_t CLIENT_BUFFER_SIZE = 2 * 1024 * 1024;

std::string Handle(std::string_view message) {
    return ">>> " + std::string(message);
}

int main(int argc, char* argv[]) {
    FNET_ASSERT(argc == 2);
    int port = std::atoll(argv[1]);

    fnet::RunServer({
        .host = "localhost",
        .port = port
    }, Handle);

    return 0;
}