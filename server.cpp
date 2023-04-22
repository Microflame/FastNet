#include <iostream>
#include <vector>
#include <fstream>

#include "fastnet.hpp"
#include "common.hpp"

fnet::Server* SERVER;

void OnSigInt(int) {
    if (!SERVER->IsRunning()) {
        exit(1);
    }
    SERVER->Stop();
}

void Handle(std::string_view message, std::string& dest) {
    dest.assign(message);
}

int main(int argc, char* argv[]) {
    FNET_ASSERT(argc == 2);
    int port = std::atoll(argv[1]);

    common::Tracer::Get();

    fnet::Server server({
        .host = "localhost",
        .port = port,
        .max_num_clients = 4,
        .reuse_addr = true,
    });
    SERVER = &server;
    signal(SIGINT, OnSigInt);

    server.Run(Handle);

    if (common::Tracer::INITIALIZED) {
        std::ofstream ofs("trace_server.json");
        FNET_ASSERT(ofs);
        ofs << common::Tracer::ToString();
    }

    return 0;
}