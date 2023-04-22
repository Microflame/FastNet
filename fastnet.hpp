#include <span>
#include <string_view>
#include <stdexcept>
#include <cstring>
#include <functional>
#include <deque>
#include <vector>
#include <memory>
#include <iostream>

#include <errno.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <netinet/tcp.h>

#include "common.hpp"


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

void DbgRecv(size_t size) {
    // static size_t total = 0;
    // static size_t cnt = 0;
    // cnt += 1;
    // total += size;
    // std::cerr << "recv: " << cnt << " (" << total << ")\n";
}

void DbgSend(size_t size) {
    // static size_t total = 0;
    // static size_t cnt = 0;
    // cnt += 1;
    // total += size;
    // std::cerr << "sent: " << cnt << " (" << total << ")\n";
}

class Span {
public:
    Span() {}

    Span(char* str, size_t len) {
        data_ = str;
        size_ = len;
    }

    Span(char* str) {
        data_ = str;
        size_ = std::strlen(str);
    }

    Span(std::string& str) {
        data_ = str.data();
        size_ = str.size();
    }

    Span(std::span<char>& str) {
        data_ = str.data();
        size_ = str.size();
    }

    char* data() const { return data_; }
    size_t size() const { return size_; }

    Span subspan(size_t offset, size_t len = size_t(-1)) {
        FNET_ASSERT(offset <= size_);
        len = std::min(len, size_ - offset);
        return Span(data_ + offset, len);
    }

private:
    char* data_ = nullptr;
    size_t size_ = 0;
};

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

    char* GetReservedStart() {
        return buffer_ + offset_;
    }

    char* GetAvailableStart() {
        return buffer_ + ((offset_ + reserved_) % buffer_size_);
    }

    char* Reserve(size_t reserved) {
        FNET_ASSERT(reserved <= Available());
        char* res = GetAvailableStart();
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

int SetSockOpt(int fd, int opt, int level, int enable = 1) {
    return setsockopt(fd, level, opt, &enable, sizeof(enable));
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

SockResult RecvFixed(int fd, char* data, size_t size) {
    size_t num_recv_total = 0;

    while (num_recv_total < size) {
        int num_recv = recv(fd, data + num_recv_total, size - num_recv_total, 0);
        switch (num_recv) {
            case 0: return {num_recv_total, SockResult::DISCONNECTED};
            case -1: return {num_recv_total, SockResult::BROKEN};
        }
        num_recv_total += num_recv;
    }
    return {size, SockResult::OK};
}

SockResult RecvFixed(int fd, Span buffer, size_t size) {
    if (buffer.size() < size) {
        return {0, SockResult::INSUFFICIENT_BUFFER};
    }
    return RecvFixed(fd, buffer.data(), size);
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
    ClientPipe() : buffer_(64 * 1024 * 1024) {}

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
        if (send_queue_.size() == 0) {
            return {0, SockResult::OK};
        }

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
                // return {num_sent_during_call, SockResult::WOULD_BLOCK};
                break;
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
            DbgSend(send_queue_.front().header.payload_size);
            sent_in_message_ -= send_queue_.front().header.GetFullSize();
            send_queue_.pop_front();
        }
        return {num_sent_during_call, SockResult::OK};
    }

    void ScheduleMessage(std::string message) {
        Header h = MakeHeader(message.size());
        send_queue_.push_back({h, std::move(message)});
    }

    // size_t GetFullSize() const { return buffer_.size(); }
    size_t GetFreeSize() const { return buffer_.size() - buffer_size_; }
    size_t GetUsedSize() const { return buffer_size_; }
    char* GetWriteBegin() { return buffer_.data() + buffer_size_; }

    void MarkConsumed(size_t consumed) {
        if (consumed == 0) {
            return;
        }
        FNET_ASSERT(consumed <= buffer_size_);
        size_t remains = buffer_size_ - consumed;
        if (remains) {
            std::memmove(buffer_.data(), buffer_.data() + consumed, remains); //TODO: use mmapped ring buffer
        }
        buffer_size_ = remains;
    }

    // Span GetFreeBuffer() {
    //     return {GetWriteBegin(), GetFreeSize()};
    // }

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
            break;
        }

        DbgRecv(header.payload_size);
        Span message_body = data.subspan(header.header_size, header.payload_size);
        client.ScheduleMessage(handler({message_body.data(), message_body.size()}));

        data = data.subspan(full_size);
        num_bytes_processed += full_size;
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
            FNET_EXIT_IF_ERROR(SetSockOpt(server_socked_fd, SO_REUSEADDR, SOL_SOCKET));
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

                    FNET_EXIT_IF_ERROR(SetSockOpt(client_fd, TCP_NODELAY, IPPROTO_TCP));
                    FNET_EXIT_IF_ERROR(SetFileFlag(client_fd, O_NONBLOCK));
                    size_t client_id = client_index_pool.Claim();
                    clients[client_id].Reset(client_fd);
                    FNET_EXIT_IF_ERROR(AddEpollEvent(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP, client_id));
                    continue;
                }

                size_t client_id = ev.data.u64;
                ClientPipe& client = clients[client_id];

                if (ev.events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                    FNET_EXIT_IF_ERROR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                    client_index_pool.Return(client_id);
                    client.Close();
                    continue;
                }

                bool had_pending_out = client.HasPendingOutgoingMessages();
                if (ev.events & EPOLLIN) {
                    SockResult res = client.Recv();
                    if (res.error == SockResult::BROKEN || res.error == SockResult::DISCONNECTED) {
                        FNET_EXIT_IF_ERROR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                        client_index_pool.Return(client_id);
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
                        client_index_pool.Return(client_id);
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

class Client;

struct RequestFuture
{
    bool is_finished = {};
    bool is_error = {};
    std::string response = {};
    Client* const client = {};

    RequestFuture(Client* client) : client(client) {}

    RequestFuture(const RequestFuture&) = delete;
    RequestFuture(RequestFuture&&) = delete;
    RequestFuture& operator=(const RequestFuture&) = delete;
    RequestFuture& operator=(RequestFuture&&) = delete;

    std::string GetResult() {
        Wait();
        return std::move(response);
    }

    void Wait();

    void SetSuccessful()
    {
        is_finished = true;
        DbgRecv(response.size());
    }

    void SetError()
    {
        is_error = true;
        is_finished = true;
    }
};

using RequestFuturePtr = std::unique_ptr<RequestFuture>;

struct HeaderSpan {
    Header header;
    Span span;
};

class Client {
    int server_socket_fd_ = -1;

    std::deque<HeaderSpan> send_queue_;
    size_t sent_in_this_iovec_ = 0;
    size_t num_iovecs_sent_ = 0;

    size_t recv_in_payload_ = 0;
    bool header_is_parsed_ = false;
    RingBuffer recv_buffer_;
    std::deque<RequestFuture*> recv_queue_;
    std::vector<char> buffer_;

public:
    Client(const std::string& host, int port, size_t buffer_size = 64 * 1024) : recv_buffer_(buffer_size), buffer_(8 * 1024 * 1024) {
        FNET_ASSERT(recv_buffer_.BufferSize() >= sizeof(Header));
        FNET_EXIT_IF_ERROR(server_socket_fd_ = socket(AF_INET, SOCK_STREAM, 0));

        sockaddr_in server_sockaddr = {};
        server_sockaddr.sin_family = AF_INET;
        server_sockaddr.sin_port = htons(port);
        server_sockaddr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

        FNET_EXIT_IF_ERROR(connect(server_socket_fd_, (const struct sockaddr*) &server_sockaddr, sizeof(server_sockaddr)));
        FNET_EXIT_IF_ERROR(SetSockOpt(server_socket_fd_, TCP_NODELAY, IPPROTO_TCP));
        // The socket is blocking by default.
        // Whenewer we want it in the non blocking mode, we'll use MSG_DONTWAIT
    }

    ~Client() {
        Stop();
    }

    void ScheduleSend(Span request) {
        send_queue_.push_back({MakeHeader(request.size()), request});
    }

    RequestFuturePtr ScheduleRecv() {
        RequestFuturePtr future = std::make_unique<RequestFuture>(this);
        recv_queue_.push_back(future.get());
        return future;
    }

    RequestFuturePtr ScheduleRequest(Span request) {
        ScheduleSend(request);
        return ScheduleRecv();
    }

    void Wait() {
        while (recv_queue_.size()) {
            DoIoCycle();
        }
    }

    std::string DoRequest(Span request) {
        FNET_ASSERT(server_socket_fd_ != -1);

        auto rf = ScheduleRequest(request);
        rf->Wait();
        FNET_ASSERT(!rf->is_error);
        return std::move(rf->response);
    }

    void DoSyncRequest(Span request, std::string& dest) {
        FNET_ASSERT(server_socket_fd_ != -1);

        Wait();

        SockResult send_res = SendMessage(server_socket_fd_, request);
        FNET_ASSERT(send_res);

        Header header = {};
        FNET_ASSERT(RecvFixed(server_socket_fd_, (char*) &header, sizeof(header)));
        dest.resize(header.payload_size);
        FNET_ASSERT(RecvFixed(server_socket_fd_, dest.data(), dest.size()));
    }

    void Stop() {
        if (server_socket_fd_ != -1) {
            close(server_socket_fd_);
            server_socket_fd_ = -1;
        }

        for (RequestFuture* rf: recv_queue_) {
            rf->SetError();
        }

        send_queue_.clear();
        recv_queue_.clear();
    }

    void DoIoCycle() {

        bool want_send = send_queue_.size();
        bool want_recv = recv_queue_.size();

        pollfd pfd = {};
        pfd.fd = server_socket_fd_;
        pfd.events = (POLLIN * want_recv) | (POLLOUT * want_send);
        auto trace_event = common::Tracer::Begin("poll");
        poll(&pfd, 1, -1);
        common::Tracer::End(trace_event);

        FNET_ASSERT(!(pfd.revents & POLLNVAL));

        if (pfd.revents & POLLERR || pfd.revents & POLLHUP) {
            Stop();
            return;
        }

        if (pfd.revents & POLLOUT) {
            std::vector<iovec> iovecs(2 * send_queue_.size());
            for (size_t i = 0; i < send_queue_.size(); ++i) {
                iovecs[2 * i].iov_base = &send_queue_[i].header;
                iovecs[2 * i].iov_len = sizeof(Header);
                iovecs[2 * i + 1].iov_base = send_queue_[i].span.data();
                iovecs[2 * i + 1].iov_len = send_queue_[i].span.size();
            }

            while (num_iovecs_sent_ < iovecs.size()) {
                while (num_iovecs_sent_ < iovecs.size() && iovecs[num_iovecs_sent_].iov_len <= sent_in_this_iovec_) {
                    sent_in_this_iovec_ -= iovecs[num_iovecs_sent_].iov_len;
                    num_iovecs_sent_ += 1;
                }

                if (num_iovecs_sent_ == iovecs.size()) {
                    break;
                }

                iovec original_iovec = iovecs[num_iovecs_sent_];
                iovecs[num_iovecs_sent_].iov_base = (char*) iovecs[num_iovecs_sent_].iov_base + sent_in_this_iovec_;
                iovecs[num_iovecs_sent_].iov_len -= sent_in_this_iovec_;

                msghdr msg = {
                    .msg_iov = &iovecs[num_iovecs_sent_],
                    .msg_iovlen = iovecs.size() - num_iovecs_sent_,
                };
                auto tev = common::Tracer::Begin("sendmsg");
                int num_sent = sendmsg(server_socket_fd_, &msg, MSG_NOSIGNAL | MSG_DONTWAIT);
                common::Tracer::End(tev);

                iovecs[num_iovecs_sent_] = original_iovec;

                if (num_sent == -1 && (errno == EAGAIN || EWOULDBLOCK)) {
                    break;
                }
                FNET_ASSERT(num_sent > 0);

                sent_in_this_iovec_ += num_sent;
            }

            size_t num_messages_sent = num_iovecs_sent_ / 2;
            num_iovecs_sent_ -= num_messages_sent * 2;
            for (size_t i = 0; i < num_messages_sent; i++) {
                DbgSend(send_queue_.front().header.payload_size);
                send_queue_.pop_front();
            }
        }

        if (pfd.revents & POLLIN) {
            while (recv_queue_.size()) {
                if (header_is_parsed_) {
                    FNET_ASSERT(recv_buffer_.ReservedSize() == 0);

                    size_t remains_in_payload = recv_queue_[0]->response.size() - recv_in_payload_;
                    if (remains_in_payload >= 1024) {
                        auto tev = common::Tracer::Begin("recv large");
                        int num_recv = recv(server_socket_fd_, recv_queue_[0]->response.data() + recv_in_payload_, remains_in_payload, MSG_NOSIGNAL | MSG_DONTWAIT);
                        common::Tracer::End(tev);
                        if (num_recv == 0) {
                            Stop();
                            return;
                        }

                        if (num_recv < 0 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                            break;
                        }
                        FNET_ASSERT(num_recv > 0);

                        recv_in_payload_ += num_recv;

                        if (recv_in_payload_ == recv_queue_[0]->response.size()) {
                            recv_queue_[0]->SetSuccessful();
                            recv_queue_.pop_front();
                            header_is_parsed_ = false;
                            recv_in_payload_ = 0;
                        }

                        continue;
                    }
                }

                char* avail = recv_buffer_.GetAvailableStart();
                size_t num_avail = recv_buffer_.Available();

                auto trace_event = common::Tracer::Begin("recv");
                int num_recv = recv(server_socket_fd_, avail, num_avail, MSG_NOSIGNAL | MSG_DONTWAIT);
                common::Tracer::End(trace_event);
                if (num_recv == 0) {
                    Stop();
                    return;
                }

                if (num_recv < 0 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                    break;
                }
                FNET_ASSERT(num_recv > 0);

                recv_buffer_.Reserve(num_recv);

                // Deliver responses to Futures
                trace_event = common::Tracer::Begin("Deliver responses");
                while (recv_queue_.size()) {
                    char* reserved = recv_buffer_.GetReservedStart();
                    size_t reserved_size = recv_buffer_.ReservedSize();

                    if (!header_is_parsed_) {
                        auto trace_event = common::Tracer::Begin("header_is_parsed_");
                        if (reserved_size >= sizeof(Header)) {
                            Header& header = *((Header*) reserved);
                            auto trace_event2 = common::Tracer::Begin("string::resize(%lld)", header.payload_size);
                            recv_queue_[0]->response.resize(header.payload_size);
                            common::Tracer::End(trace_event2);
                            header_is_parsed_ = true;
                            recv_buffer_.Release(sizeof(Header));
                            common::Tracer::End(trace_event);
                            continue;
                        } else {
                            common::Tracer::End(trace_event);
                            break;
                        }
                        common::Tracer::End(trace_event);
                    }

                    size_t remains_in_payload = recv_queue_[0]->response.size() - recv_in_payload_;

                    if (reserved_size == 0 && remains_in_payload) {
                        break;
                    }

                    size_t num_to_copy = std::min(remains_in_payload, reserved_size);
                    auto trace_event = common::Tracer::Begin("memcpy");
                    std::memcpy(recv_queue_[0]->response.data() + recv_in_payload_, reserved, num_to_copy);
                    common::Tracer::End(trace_event);
                    recv_buffer_.Release(num_to_copy);
                    recv_in_payload_ += num_to_copy;

                    if (recv_in_payload_ == recv_queue_[0]->response.size()) {
                        recv_queue_[0]->SetSuccessful();
                        recv_queue_.pop_front();
                        header_is_parsed_ = false;
                        recv_in_payload_ = 0;
                    }
                }
                common::Tracer::End(trace_event);
            }
        }
    }
};

void RequestFuture::Wait() {
    while (!is_finished) {
        auto tev = common::Tracer::Begin("client->DoIoCycle");
        client->DoIoCycle();
        common::Tracer::End(tev);
    }
}

} // namespace fnet

