#include <span>
#include <string_view>
#include <stdexcept>
#include <cstring>
#include <functional>
#include <deque>
#include <vector>

#include <stdio.h>
#include <netinet/ip.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <fcntl.h>


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

template <typename F> 
struct DeferImpl {
    DeferImpl(F f): fn(f) {}
    ~DeferImpl() { fn(); }
    F fn;
};

#define FNET_CONCAT_IMPL(a, b) a ## b
#define FNET_CONCAT(a, b) FNET_CONCAT_IMPL(a, b)
#define FNET_DEFER DeferImpl FNET_CONCAT(defer_, __LINE__) = [&]()

namespace fnet
{

using Span = std::span<char>;

static const size_t PAGE_SIZE = getpagesize();

void PrintCError(const char* file, int line) {
    char ERROR_BUFFER[1024] = {};
    ERROR_BUFFER[0] = 0;
    snprintf(ERROR_BUFFER, sizeof(ERROR_BUFFER), "%s:%d", file, line);
    perror(ERROR_BUFFER);
}

class RingBuffer {
public:
    RingBuffer(size_t size) : buffer_size_(size) {
        FNET_ASSERT(buffer_size_ > 0);
        FNET_ASSERT(buffer_size_ % PAGE_SIZE == 0);

        int fd = memfd_create("RingBuffer", 0);
        FNET_EXIT_IF_ERROR(fd);
        FNET_EXIT_IF_ERROR(ftruncate(fd, buffer_size_));

        buffer_ = (char*) mmap(nullptr, 2 * buffer_size_, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        FNET_EXIT_IF_ERROR(buffer_);

        FNET_EXIT_IF_ERROR(mmap(buffer_, buffer_size_, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0));
        FNET_EXIT_IF_ERROR(mmap(buffer_ + buffer_size_, buffer_size_, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0));

        FNET_EXIT_IF_ERROR(close(fd));
    }

    ~RingBuffer() {
        if (buffer_) {
            FNET_EXIT_IF_ERROR(munmap(buffer_, 2 * buffer_size_));
            buffer_ = nullptr;
        }
    }

    size_t BufferSize() const { return buffer_size_; }
    size_t ReservedSize() const { return reserved_; }
    size_t Available() const { return buffer_size_ - reserved_; }

    char* Reserve(size_t reserved) {
        FNET_ASSERT(reserved <= Available());
        char* res = buffer_ + ((offset_ + reserved_) % buffer_size_);
        reserved_ += reserved;
        return res;
    }

    void Release(size_t released) {
        FNET_ASSERT(released <= reserved_);
        offset_ = (offset_ + released) % buffer_size_;
        reserved_ -= released;
    }

private:
    size_t buffer_size_ = 0;
    size_t offset_ = 0;
    size_t reserved_ = 0;
    char* buffer_ = nullptr;
};

int SetFileFlag(int fd, int flag) {
    int old = fcntl(fd, F_GETFL);
    if (old == -1) return -1;
    return fcntl(fd, F_SETFL, old | flag);
}

int AddEpollEvent(int epoll_fd, int target_fd, int events_mask, uint64_t data) {
    epoll_event ev = {};
    ev.events = events_mask;
    ev.data.u64 = data;
    return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, target_fd, &ev);
}

int ModEpollEvents(int epoll_fd, int target_fd, int events_mask, uint64_t data) {
    epoll_event ev = {};
    ev.events = events_mask;
    ev.data.u64 = data;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, target_fd, &ev);
}

struct Header {
    uint16_t header_size;
    uint8_t header_version;
    uint64_t payload_size;

    size_t GetFullSize() const { return header_size + payload_size; }
};

Header MakeHeader(size_t payload_size) {
    Header header {
        .header_size = sizeof(Header),
        .header_version = 0,
        .payload_size = payload_size
    };
    return header;
}

in_addr_t MakeIpAddr(uint32_t b1, uint32_t b2, uint32_t b3, uint32_t b4) {
    return (b4 << 24) | (b3 << 16) | (b2 << 8) | b1;
}


struct SockResult {
    size_t size;
    enum Type {
        OK,
        DISCONNECTED,
        BROKEN,
        INSUFFICIENT_BUFFER,
        WOULD_BLOCK
    } error;

    operator bool() const { return error == OK; }
};

SockResult SendMessage(int fd, const Span& payload) {
    if (payload.size() == 0) {
        return {0, SockResult::OK};
    }
    Header header = MakeHeader(payload.size());
    
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

    size_t num_send_total = 0;
    while (remains_in_payload) {
        int num_send = sendmsg(fd, &msg, MSG_NOSIGNAL);
        FNET_ASSERT(num_send != 0);
        if (num_send == -1) {
            FNET_EXIT_IF_ERROR(num_send);
            return {num_send_total, SockResult::BROKEN};
        }
        num_send_total += num_send;
        if (num_send > (int) remains_in_header) {
            num_send -= remains_in_header;
            remains_in_header = 0;
            remains_in_payload -= num_send;
        }
    }
    return {num_send_total, SockResult::OK};
}

SockResult RecvFixed(int fd, Span buffer) {
    size_t num_to_recv = buffer.size();

    char* data = buffer.data();
    while (num_to_recv) {
        int num_recv = recv(fd, data, num_to_recv, 0);
        switch (num_recv) {
            case 0: return {buffer.size() - num_to_recv, SockResult::DISCONNECTED};
            case -1: return {buffer.size() - num_to_recv, SockResult::BROKEN};
        }
        num_to_recv -= num_recv;
        data += num_recv;
    }
    return {buffer.size(), SockResult::OK};
}

SockResult RecvFixed(int fd, Span buffer, size_t size) {
    if (buffer.size() < size) {
        return {0, SockResult::INSUFFICIENT_BUFFER};
    }
    return RecvFixed(fd, buffer.subspan(0, size));
}

SockResult RecvMessage(int fd, Span buffer) {
    size_t recv_total = 0;
    SockResult res = RecvFixed(fd, buffer, sizeof(Header));
    if (!res) return res;
    recv_total += res.size;

    Header& header = *( (Header*) buffer.data() );

    size_t num_to_recv = header.payload_size + header.header_size - res.size;
    Span remains = buffer.subspan(res.size);
    res = RecvFixed(fd, remains, num_to_recv);
    recv_total += res.size;
    return {recv_total, res.error};
}

class IndexPool {
public:
    IndexPool(size_t size) : pool_(size), num_available_(size) {
        for (size_t i = 0; i < size; i++) {
            pool_[i] = size - i - 1;
        }
    }

    size_t NumAvailable() {
        return num_available_;
    }

    size_t Claim() {
        FNET_ASSERT(num_available_);
        num_available_ -= 1;
        return pool_[num_available_];
    }

    void Return(size_t value) {
        FNET_ASSERT(num_available_ < pool_.size());
        pool_[num_available_] = value;
        num_available_ += 1;
    }

private:
    std::vector<size_t> pool_;
    size_t num_available_;
};

struct OutgoingMessage {
    Header header;
    std::string message;
};

class ClientPipe {
public:
    ClientPipe() : buffer_(2 * 1024 * 1024) {}

    ClientPipe(const ClientPipe& other) = delete;

    ClientPipe(ClientPipe&& other) {
        fd_ = other.fd_;
        buffer_ = std::move(other.buffer_);
        buffer_size_ = other.buffer_size_;
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
    }

    ~ClientPipe() {
        Close();
    }

    ClientPipe& operator=(const ClientPipe& other) = delete;

    ClientPipe& operator=(ClientPipe&& other) {
        Close();
        fd_ = other.fd_;
        buffer_ = std::move(other.buffer_);
        buffer_size_ = other.buffer_size_;
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
        return *this;
    }

    size_t GetFullSize() const { return buffer_.size(); }
    size_t GetFreeSize() const { return buffer_.size() - buffer_size_; }
    size_t GetUsedSize() const { return buffer_size_; }
    char* GetWriteBegin() { return buffer_.data() + buffer_size_; }
    bool HasPendingOutgoingMessages() const { return send_queue_.size(); }

    SockResult Recv() {
        size_t num_read_total = 0;
        while (GetFreeSize()) {
            int recv_res = recv(fd_, GetWriteBegin(), GetFreeSize(), 0);
            if (recv_res == -1 && (errno == EAGAIN || EWOULDBLOCK)) {
                return {num_read_total, SockResult::WOULD_BLOCK};
            }
            FNET_EXIT_IF_ERROR(recv_res);
            if (recv_res == 0) {
                return {num_read_total, SockResult::DISCONNECTED};
            }
            num_read_total += recv_res;
            buffer_size_ += recv_res;
        }
        return {num_read_total, SockResult::INSUFFICIENT_BUFFER};
    }

    SockResult Send() {
        FNET_ASSERT(send_queue_.size());

        size_t num_sent_during_call = 0;
        std::vector<iovec> iovecs(2 * send_queue_.size());
        for (size_t i = 0; i < send_queue_.size(); ++i) {
            iovecs[2 * i].iov_base = &send_queue_[i].header;
            iovecs[2 * i].iov_len = sizeof(Header);
            iovecs[2 * i + 1].iov_base = send_queue_[i].message.data();
            iovecs[2 * i + 1].iov_len = send_queue_[i].message.size();
        }

        size_t num_iovecs_sent = 0;
        size_t sent_in_iovec = sent_in_message_;
        if (sent_in_iovec > sizeof(Header)) {
            sent_in_iovec -= sizeof(Header);
            num_iovecs_sent = 1;
        }
        iovecs[num_iovecs_sent].iov_base = (char*) iovecs[num_iovecs_sent].iov_base + sent_in_iovec;
        iovecs[num_iovecs_sent].iov_len -= sent_in_iovec;

        while (iovecs.size() > num_iovecs_sent) {
            msghdr msg = {
                .msg_iov = &iovecs[num_iovecs_sent],
                .msg_iovlen = iovecs.size() - num_iovecs_sent,
            };
            int num_sent = sendmsg(fd_, &msg, MSG_NOSIGNAL);
            FNET_ASSERT(num_sent != 0);
            if (num_sent == -1 && (errno == EAGAIN || EWOULDBLOCK)) {
                return {num_sent_during_call, SockResult::WOULD_BLOCK};
            }
            if (num_sent == -1) {
                return {num_sent_during_call, SockResult::BROKEN};
            }
            num_sent_during_call += num_sent;
            sent_in_message_ += num_sent;

            while (num_sent && int(iovecs[num_iovecs_sent].iov_len) <= num_sent) {
                num_sent -= iovecs[num_iovecs_sent].iov_len;
                num_iovecs_sent += 1;
            }

            if (num_sent) {
                iovecs[num_iovecs_sent].iov_base = (char*) iovecs[num_iovecs_sent].iov_base + num_sent;
                iovecs[num_iovecs_sent].iov_len -= num_sent;
            }
        }

        for (size_t i = 0; i < num_iovecs_sent / 2; i++) {
            sent_in_message_ -= send_queue_.front().header.GetFullSize();
            send_queue_.pop_front();
        }
        return {num_sent_during_call, SockResult::OK};
    }

    void ScheduleMessage(std::string message) {
        Header h = MakeHeader(message.size());
        send_queue_.push_back({h, std::move(message)});
    }

    void MarkConsumed(size_t consumed) {
        FNET_ASSERT(consumed <= buffer_size_);
        size_t remains = buffer_size_ - consumed;
        if (remains) {
            std::memmove(buffer_.data(), buffer_.data() + consumed, remains); //TODO: use mmapped ring buffer
        }
        buffer_size_ = remains;
    }

    Span GetFreeBuffer() {
        return {GetWriteBegin(), GetFreeSize()};
    }

    Span GetUsedBuffer() {
        return {buffer_.data(), buffer_size_};
    }

    void Close() {
        if (fd_ != -1) {
            FNET_EXIT_IF_ERROR( close(fd_) );
            fd_ = -1;
        }
        buffer_size_ = 0;
        sent_in_message_ = 0;
        send_queue_ = {};
    }

    void Reset(int fd) {
        Close();
        fd_ = fd;
    }

    int Fd() const { return fd_; }

private:
    int fd_ = -1;
    std::vector<char> buffer_;
    size_t buffer_size_ = 0;
    std::deque<OutgoingMessage> send_queue_;
    size_t sent_in_message_ = 0;
};

using HandleFn = std::function<std::string(std::string_view)>;

void ProcessIncomingTraffic(ClientPipe& client, const HandleFn& handler) {
    Span data = client.GetUsedBuffer();
    size_t num_bytes_processed = 0;
    while (data.size() >= sizeof(Header)) {
        Header& header = *( (Header*) data.data() );
        size_t full_size = header.GetFullSize();
        if (data.size() < full_size) {
            return;
        }

        Span message_body = data.subspan(header.header_size, header.payload_size);
        client.ScheduleMessage(handler({message_body.data(), message_body.size()}));

        data = data.subspan(full_size);
        num_bytes_processed = full_size;
    }

    client.MarkConsumed(num_bytes_processed);
}

struct ServerConfig {
    std::string host;
    int port = -1;
    size_t max_num_clients = 128;
    bool reuse_addr = false;
};

class Server {
    ServerConfig conf_;

public:
    Server(ServerConfig conf) : conf_(std::move(conf)) {}

    void Run(const HandleFn& handler) {
        int server_socked_fd = 0;
        FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));
        FNET_DEFER { FNET_EXIT_IF_ERROR(close(server_socked_fd)); };
        FNET_EXIT_IF_ERROR(SetFileFlag(server_socked_fd, O_NONBLOCK));

        if (conf_.reuse_addr) {
            int enable = 1;
            FNET_EXIT_IF_ERROR(setsockopt(server_socked_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)));
        }

        sockaddr_in server_socked_addr = {};
        server_socked_addr.sin_family = AF_INET;
        server_socked_addr.sin_port = htons(conf_.port);
        server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

        FNET_EXIT_IF_ERROR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

        FNET_EXIT_IF_ERROR(listen(server_socked_fd, 16));

        int epoll_fd = 0;
        FNET_EXIT_IF_ERROR(epoll_fd = epoll_create(1));
        FNET_DEFER { FNET_EXIT_IF_ERROR(close(epoll_fd)); };

        FNET_EXIT_IF_ERROR(AddEpollEvent(epoll_fd, server_socked_fd, EPOLLIN, -1));

        constexpr size_t MAX_NUM_EPOLL_EVENTS = 64;
        struct epoll_event epoll_events[MAX_NUM_EPOLL_EVENTS] = {};

        std::vector<ClientPipe> clients(conf_.max_num_clients);
        IndexPool client_index_pool(conf_.max_num_clients);
        while (true) {
            int epoll_wait_res = epoll_wait(epoll_fd, epoll_events, MAX_NUM_EPOLL_EVENTS, -1);
            if (epoll_wait_res == EINTR) continue;
            FNET_EXIT_IF_ERROR(epoll_wait_res);

            for (int i = 0; i < epoll_wait_res; i++) {
                struct epoll_event& ev = epoll_events[i];

                if (ev.data.u64 == uint64_t(-1)) {
                    int client_fd = 0;
                    FNET_EXIT_IF_ERROR(client_fd = accept(server_socked_fd, nullptr, nullptr));
                    if (!client_index_pool.NumAvailable()) {
                        FNET_EXIT_IF_ERROR(close(client_fd));
                        fprintf(stderr, "Client rejected.");
                        continue;
                    }

                    SetFileFlag(client_fd, O_NONBLOCK);
                    size_t client_id = client_index_pool.Claim();
                    clients[client_id].Reset(client_fd);
                    FNET_EXIT_IF_ERROR(AddEpollEvent(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP, client_id));
                    continue;
                }

                size_t client_id = ev.data.u64;
                ClientPipe& client = clients[client_id];

                if (ev.events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                    FNET_EXIT_IF_ERROR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                    client.Close();
                    continue;
                }

                bool had_pending_out = client.HasPendingOutgoingMessages();
                if (ev.events & EPOLLIN) {
                    SockResult res = client.Recv();
                    if (res.error == SockResult::BROKEN || res.error == SockResult::DISCONNECTED) {
                        FNET_EXIT_IF_ERROR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                        client.Close();
                        continue;
                    }

                    ProcessIncomingTraffic(client, handler);
                }
                bool writes_appeared = !had_pending_out && client.HasPendingOutgoingMessages();

                if (writes_appeared || (ev.events & EPOLLOUT)) {
                    SockResult res = client.Send();
                    if (res.error == SockResult::BROKEN) {
                        FNET_EXIT_IF_ERROR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                        client.Close();
                        continue;
                    }
                }

                int event_mask = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
                if (client.HasPendingOutgoingMessages()) {
                    event_mask |= EPOLLOUT;
                }

                FNET_EXIT_IF_ERROR(ModEpollEvents(epoll_fd, client.Fd(), event_mask, client_id));
            }
        }
    }
};

void RunServer(ServerConfig conf, std::function<std::string(std::string_view)> handle) {
    Server(std::move(conf)).Run(std::move(handle));
}

class Client {
    int server_socket_fd_ = -1;
    std::vector<char> buffer_;

public:
    Client(const std::string& host, int port, size_t buffer_size = 2 * 1024 * 1024) {
        buffer_.resize(buffer_size);

        FNET_EXIT_IF_ERROR(server_socket_fd_ = socket(AF_INET, SOCK_STREAM, 0));

        sockaddr_in server_sockaddr = {};
        server_sockaddr.sin_family = AF_INET;
        server_sockaddr.sin_port = htons(port);
        server_sockaddr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

        FNET_EXIT_IF_ERROR(connect(server_socket_fd_, (const struct sockaddr*) &server_sockaddr, sizeof(server_sockaddr)));
    }

    ~Client() {
        Disconnect();
    }

    std::string_view DoRequest(Span request) {
        if (server_socket_fd_ == -1) {
            throw std::runtime_error("Not connected");
        }

        SockResult send_res = SendMessage(server_socket_fd_, request);
        if (!send_res) return {};
        SockResult recv_res = RecvMessage(server_socket_fd_, {buffer_.data(), buffer_.size()});
        if (!recv_res) return {};

        Header& header = *( (Header*) buffer_.data() );
        return std::string_view(buffer_.data() + header.header_size, header.payload_size);
    }

    std::string_view DoRequest(std::string& str) {
        return DoRequest({str.data(), str.size()});
    }

    void Disconnect() {
        if (server_socket_fd_ != -1) {
            close(server_socket_fd_);
            server_socket_fd_ = -1;
        }
    }
};

} // namespace fnet

