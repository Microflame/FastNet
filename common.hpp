#include <netinet/ip.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include <span>
#include <cstring>

#define FNET_EXIT_IF_ERROR(x)                                           \
    do {                                                                \
        if ((x) == -1) {                                                \
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

struct Header {
    uint16_t header_size;
    uint8_t header_version;
    uint64_t payload_size;
};

struct Message {
    Header header;
    std::string_view payload;
};

Header MakeHeader(const std::span<char>& payload) {
    Header header {
        .header_size = sizeof(Header),
        .header_version = 0,
        .payload_size = payload.size()
    };
    return header;
}

void InitProcess() {
    struct sigaction sa = {};
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, nullptr);
}

void PrintCError(const char* file, int line) {
    char ERROR_BUFFER[1024] = {};
    ERROR_BUFFER[0] = 0;
    snprintf(ERROR_BUFFER, sizeof(ERROR_BUFFER), "%s:%d", file, line);
    perror(ERROR_BUFFER);
}

in_addr_t MakeIpAddr(uint32_t b1, uint32_t b2, uint32_t b3, uint32_t b4) {
    return (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
}


void SendMessage(int fd, const std::span<char>& payload, std::span<char>& buffer) {
    Header header = MakeHeader(payload);
    FNET_ASSERT(payload.size() + sizeof(Header) <= buffer.size());
    
    char* data = buffer.data();
    *((Header*) data) = header;
    data += sizeof(Header);
    std::memcpy(data, payload.data(), payload.size());
    data = buffer.data();
    size_t remains = payload.size() + sizeof(Header);

    while (remains) {
        // TODO: Try sendmsg
        int num_send = send(fd, data, remains, 0);
        FNET_ASSERT(num_send > 0);
        remains -= num_send;
        data += num_send;
    }
}

std::span<char> RecvFixed(int fd, size_t size, std::span<char> buffer) {
    FNET_ASSERT(size <= buffer.size());

    char* data = buffer.data();
    while (size) {
        int num_recv = 0;
        FNET_EXIT_IF_ERROR(num_recv = recv(fd, data, size, 0));
        size -= num_recv;
        data += num_recv;
    }
    return std::span<char>(buffer.data() + size, buffer.size() - size);
}

std::string_view RecvMessage(int fd, std::span<char> buffer) {
    std::span<char> remains = RecvFixed(fd, sizeof(Header), buffer);
    Header& header = *( (Header*) buffer.data() );
    size_t num_to_recv = header.payload_size + header.header_size - sizeof(Header);
    RecvFixed(fd, num_to_recv, remains);
    return std::string_view(buffer.data() + header.header_size, header.payload_size);
}