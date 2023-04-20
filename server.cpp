#include <iostream>
#include <vector>

#include "fastnet.hpp"

std::string Handle(std::string_view message) {
    return std::string(message);
    // return ">>> " + std::string(message);
    // return "OK";
}

int main(int argc, char* argv[]) {
    FNET_ASSERT(argc == 2);
    int port = std::atoll(argv[1]);

    fnet::RunServer({
        .host = "localhost",
        .port = port,
        .max_num_clients = 4,
        .reuse_addr = true,
    }, Handle);

    return 0;
}