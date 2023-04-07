#include <netinet/ip.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include <span>
#include <string_view>
#include <stdexcept>
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

template <typename T, typename E>
class Result {
public:
    static Result MakeError(E error) {
        return Result({}, std::move(error));
    }

    static Result MakeOk(T result) {
        return Result(std::move(result), {});
    }

    Result(T result, E error) :
        result_(std::move(result)),
        error_(std::move(error)) {}

    operator bool() const { return error_; }
private:
    T result_;
    E error_;
};

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
        int num_send = send(fd, data, remains, 0);
        FNET_ASSERT(num_send > 0);
        remains -= num_send;
        data += num_send;
    }
}

void SendMessage_sendmsg(int fd, const std::span<char>& payload) {
    Header header = MakeHeader(payload);
    
    iovec msg_iov[2] = {
        iovec{.iov_base = &header, .iov_len = sizeof(Header)},
        iovec{.iov_base = payload.data(), .iov_len = payload.size()},
    };
    msghdr msg = {
        .msg_iov = msg_iov,
        .msg_iovlen = 2,
    };
    size_t& remains_in_header = msg_iov[0].iov_len;
    size_t& remains_in_payload = msg_iov[1].iov_len;

    while (remains_in_payload) {
        int num_send = sendmsg(fd, &msg, 0);
        FNET_ASSERT(num_send > 0);
        if (num_send > (int) remains_in_header) {
            num_send -= remains_in_header;
            remains_in_header = 0;
            remains_in_payload -= num_send;
        }
    }
}

struct RecvResult {
    size_t size;
    enum Type {
        OK,
        DISCONNECTED,
        BROKEN,
        INSUFFICIENT_BUFFER
    } error;

    operator bool() const { return error == OK; }
};

[[nodiscard]] RecvResult RecvFixed(int fd, std::span<char> buffer) {
    size_t num_to_recv = buffer.size();

    char* data = buffer.data();
    while (num_to_recv) {
        int num_recv = recv(fd, data, num_to_recv, 0);
        switch (num_recv) {
            case 0: return {buffer.size() - num_to_recv, RecvResult::DISCONNECTED};
            case -1: return {buffer.size() - num_to_recv, RecvResult::BROKEN};
        }
        num_to_recv -= num_recv;
        data += num_recv;
    }
    return {buffer.size(), RecvResult::OK};
}

[[nodiscard]] RecvResult RecvFixed(int fd, std::span<char> buffer, size_t size) {
    if (buffer.size() < size) {
        return {0, RecvResult::INSUFFICIENT_BUFFER};
    }
    return RecvFixed(fd, buffer.subspan(0, size));
}

[[nodiscard]] RecvResult RecvMessage(int fd, std::span<char> buffer) {
    size_t total_size = 0;
    RecvResult res = RecvFixed(fd, buffer, sizeof(Header));
    if (!res) return res;
    total_size += res.size;

    Header& header = *( (Header*) buffer.data() );

    size_t num_to_recv = header.payload_size + header.header_size - res.size;
    std::span<char> remains = buffer.subspan(res.size);
    res = RecvFixed(fd, remains, num_to_recv);
    total_size += res.size;
    return {total_size, res.error};
}
