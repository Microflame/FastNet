#include <span>
#include <string_view>
#include <stdexcept>
#include <cstring>
#include <functional>
#include <deque>
#include <vector>
#include <memory>
#include <iostream>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>

#include "common.hpp"


// #define FNET_DEBUG 1

namespace fnet {

#define FNET_CONCAT_IMPL(a, b) a ## b
#define FNET_CONCAT(a, b) FNET_CONCAT_IMPL(a, b)

#define FNET_STRINGIZE_IMPL(x) #x
#define FNET_STRINGIZE(x) FNET_STRINGIZE_IMPL(x)

#define FNET_THROW(err)                                                 \
    do {                                                                \
        std::string location = __FILE__":"                              \
                               FNET_STRINGIZE(__LINE__) ": ";           \
        throw std::runtime_error(location + err);                       \
    } while (0)

std::string ErrnoToString(int);
#define FNET_THROW_IF_POSIX_ERR(x)                                      \
    do {                                                                \
        if (int64_t(x) == -1) {                                         \
            FNET_THROW(ErrnoToString(errno));                           \
        }                                                               \
    } while (0)

#define FNET_ASSERT(x)                                                  \
    do {                                                                \
        if (!(x)) {                                                     \
            FNET_THROW("(" #x ") was not fulfilled");                   \
        }                                                               \
    } while (0)

template <typename F> 
struct DeferImpl;
#define FNET_DEFER ::fnet::DeferImpl FNET_CONCAT(defer_, __LINE__) = [&]()

#ifdef FNET_DEBUG

void DbgRecv(size_t size) {
    static size_t total = 0;
    static size_t cnt = 0;
    cnt += 1;
    total += size;
    std::cerr << "recv: " << cnt << " (" << total << ")\n";
}

void DbgSend(size_t size) {
    static size_t total = 0;
    static size_t cnt = 0;
    cnt += 1;
    total += size;
    std::cerr << "sent: " << cnt << " (" << total << ")\n";
}

#define FNET_DBG_RECV(size) DbgRecv(size)
#define FNET_DBG_SEND(size) DbgSend(size)

#else // #ifdef FNET_DEBGU

#define FNET_DBG_RECV(size)
#define FNET_DBG_SEND(size)

#endif

// TODO: Check if this works
#define FNET_TRACE_SCOPE(name) \
    size_t FNET_CONCAT(FNET_trace_scope_, __LINE__) = common::Tracer::Begin(name); \
    FNET_DEFER { common::Tracer::End(FNET_CONCAT(FNET_trace_scope_, __LINE__)); }


static const size_t PAGE_SIZE = getpagesize();


template <typename F> 
struct DeferImpl {
    DeferImpl(F f): fn(f) {}
    ~DeferImpl() { fn(); }
    F fn;
};

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

class RingBuffer {
public:
    RingBuffer() {}

    RingBuffer(size_t size) : buffer_size_(size) {
        FNET_ASSERT(buffer_size_ > 0);
        FNET_ASSERT(buffer_size_ % PAGE_SIZE == 0);

        int fd = memfd_create("RingBuffer", 0);
        FNET_THROW_IF_POSIX_ERR(fd);
        FNET_THROW_IF_POSIX_ERR(ftruncate(fd, buffer_size_));

        buffer_ = (char*) mmap(nullptr, 2 * buffer_size_, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
        FNET_THROW_IF_POSIX_ERR(buffer_);

        FNET_THROW_IF_POSIX_ERR(mmap(buffer_, buffer_size_, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0));
        FNET_THROW_IF_POSIX_ERR(mmap(buffer_ + buffer_size_, buffer_size_, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, fd, 0));

        FNET_THROW_IF_POSIX_ERR(close(fd));
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&& other) {
        buffer_size_ = other.buffer_size_;
        offset_ = other.offset_;
        reserved_ = other.reserved_;
        buffer_ = other.buffer_;
        
        other.buffer_ = nullptr;
    }

    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer& operator=(RingBuffer&& other) {
        Destroy();

        buffer_size_ = other.buffer_size_;
        offset_ = other.offset_;
        reserved_ = other.reserved_;
        buffer_ = other.buffer_;
        
        other.buffer_ = nullptr;

        return *this;
    }

    ~RingBuffer() {
        Destroy();
    }

    size_t GetNumTotal() const { return buffer_size_; }
    size_t GetNumReserved() const { return reserved_; }
    size_t GetNumFree() const { return buffer_size_ - reserved_; }

    char* GetReservedStart() {
        return buffer_ + offset_;
    }

    char* GetFreeStart() {
        return buffer_ + ((offset_ + reserved_) % buffer_size_);
    }

    char* Reserve(size_t reserved) {
        FNET_ASSERT(reserved <= GetNumFree());
        char* res = GetFreeStart();
        reserved_ += reserved;
        return res;
    }

    void Release(size_t released) {
        FNET_ASSERT(released <= reserved_);
        offset_ = (offset_ + released) % buffer_size_;
        reserved_ -= released;
    }

    void Clear() {
        reserved_ = 0;
    }

    void Destroy() {
        if (buffer_) {
            FNET_THROW_IF_POSIX_ERR(munmap(buffer_, 2 * buffer_size_));
            buffer_ = nullptr;
        }
    }

private:
    size_t buffer_size_ = 0;
    size_t offset_ = 0;
    size_t reserved_ = 0;
    char* buffer_ = nullptr;
};

template <typename T>
struct ObjectPool {
    std::vector<T> available_;

    T Claim() {
        return Claim([]() -> T { return {}; })
    }

    template <typename F>
    T Claim(F& factory) {
        if (available_.size() == 0) {
            return factory();
        }

        T res = std::move(available_.back());
        available_.pop_back();
        return res;
    }

    void Return(T&& object) {
        available_.emplace_back(std::move(object));
    }
};


std::string ErrnoToString(int errnum) {
    char ERROR_BUFFER[1024] = {};
    ERROR_BUFFER[0] = 0;
    const char* err_str = strerror_r(errnum, ERROR_BUFFER, sizeof(ERROR_BUFFER));

    return {err_str};
}

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

int ModEpollEvent(int epoll_fd, int target_fd, int events_mask, uint64_t data) {
    epoll_event ev = {};
    ev.events = events_mask;
    ev.data.u64 = data;
    return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, target_fd, &ev);
}

in_addr_t ResolveHostname(const char* hostname) {
    addrinfo* getaddrinfo_res = nullptr;
    addrinfo hints = {};
    hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    FNET_THROW_IF_POSIX_ERR(getaddrinfo(hostname, nullptr, &hints, &getaddrinfo_res));
    FNET_DEFER { freeaddrinfo(getaddrinfo_res); };

    FNET_ASSERT(getaddrinfo_res);
    FNET_ASSERT(getaddrinfo_res->ai_addr);
    in_addr_t res = {};
    inet_pton(AF_INET, getaddrinfo_res->ai_addr->sa_data, &res);
    return res;
}

sockaddr_in MakeSockaddr(const char* hostname, int port) {
    sockaddr_in sockaddr = {};
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    sockaddr.sin_addr.s_addr = ResolveHostname(hostname);
    return sockaddr;
}


/**
 * |      MESSAGE       |
 * | HEADER |  PAYLOAD  |
*/
struct Header {
    uint8_t version;
    uint8_t reserved_1_;
    uint16_t reserved_2_;
    uint32_t payload_size;

    size_t GetMessageSize() const { return sizeof(Header) + payload_size; }
};

static_assert(sizeof(Header) == 8);

Header MakeHeader(uint32_t payload_size) {
    Header header = {};
    header.payload_size = payload_size;
    return header;
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
    while (remains_in_payload + remains_in_header) {
        int num_send = sendmsg(fd, &msg, MSG_NOSIGNAL);
        FNET_ASSERT(num_send != 0);
        FNET_THROW_IF_POSIX_ERR(num_send);
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

struct OutgoingMessage {
    Header header;
    std::string message;

    OutgoingMessage(Header header, std::string message)
    : header(header),
      message(std::move(message))
    {}
};

using HandleFn = std::function<void(std::string_view, std::string&)>;

class ClientConnection {
public:
    ClientConnection() = delete;
    ClientConnection(const ClientConnection& other) = delete;
    ClientConnection& operator=(const ClientConnection& other) = delete;

    ClientConnection(size_t buffer_size) : buffer_(buffer_size) {}

    // TODO: Fix this code duplication.
    // There is not real need in move ctors
    ClientConnection(ClientConnection&& other) {
        fd_ = other.fd_;
        other.fd_ = -1;
        buffer_ = std::move(other.buffer_);
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
    }

    ~ClientConnection() {
        Close();
    }

    ClientConnection& operator=(ClientConnection&& other) {
        Close();
        fd_ = other.fd_;
        other.fd_ = -1;
        buffer_ = std::move(other.buffer_);
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
        return *this;
    }

    size_t GetSendQueueSize() const { return send_queue_.size(); }

    SockResult Recv() {
        FNET_TRACE_SCOPE("ClientConnection::Recv()");
        size_t num_read_total = 0;
        while (buffer_.GetNumFree()) {
            int recv_res = 0;
            {
                FNET_TRACE_SCOPE("recv");
                recv_res = recv(fd_, buffer_.GetFreeStart(), buffer_.GetNumFree(), 0);
            }

            if (recv_res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return {num_read_total, SockResult::WOULD_BLOCK};
            }
            FNET_THROW_IF_POSIX_ERR(recv_res);
            if (recv_res == 0) {
                return {num_read_total, SockResult::DISCONNECTED};
            }

            num_read_total += recv_res;
            buffer_.Reserve(recv_res);
        }
        return {num_read_total, SockResult::INSUFFICIENT_BUFFER};
    }

    SockResult Send() {
        FNET_TRACE_SCOPE("ClientConnection::Send()");
        if (send_queue_.size() == 0) {
            return {0, SockResult::OK};
        }

        auto trace_event_build_iovecs = common::Tracer::Begin("Send:BuildIovecs");
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
        common::Tracer::End(trace_event_build_iovecs);

        while (iovecs.size() > num_iovecs_sent) {
            msghdr msg = {
                .msg_iov = &iovecs[num_iovecs_sent],
                .msg_iovlen = iovecs.size() - num_iovecs_sent,
            };
            auto trace_event_sendmsg = common::Tracer::Begin("sendmsg");
            int num_sent = sendmsg(fd_, &msg, MSG_NOSIGNAL);
            common::Tracer::End(trace_event_sendmsg);
            FNET_ASSERT(num_sent != 0 || msg.msg_iovlen == 0 || msg.msg_iov[0].iov_len == 0);
            if (num_sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                break;
            }
            FNET_THROW_IF_POSIX_ERR(num_sent);
            if (num_sent == -1) {
                return {num_sent_during_call, SockResult::BROKEN};
            }
            num_sent_during_call += num_sent;
            sent_in_message_ += num_sent;

            while (num_iovecs_sent < iovecs.size() && int(iovecs[num_iovecs_sent].iov_len) <= num_sent) {
                num_sent -= iovecs[num_iovecs_sent].iov_len;
                num_iovecs_sent += 1;
            }

            if (num_sent) {
                iovecs[num_iovecs_sent].iov_base = (char*) iovecs[num_iovecs_sent].iov_base + num_sent;
                iovecs[num_iovecs_sent].iov_len -= num_sent;
            }
        }

        for (size_t i = 0; i < num_iovecs_sent / 2; i++) {
            FNET_DBG_SEND(send_queue_.front().header.payload_size);
            sent_in_message_ -= send_queue_.front().header.GetMessageSize();
            string_pool_.Return(std::move(send_queue_.front().message));
            send_queue_.pop_front();
        }
        return {num_sent_during_call, SockResult::OK};
    }

    void ProcessIncomingTraffic(const HandleFn& handler) {
        FNET_TRACE_SCOPE("ClientConnection::ProcessIncomingTraffic()");
        Span data = GetUsedBuffer();
        size_t num_bytes_processed = 0;
        while (data.size() >= sizeof(Header)) {
            Header& header = *( (Header*) data.data() );
            size_t full_size = header.GetMessageSize();
            if (data.size() < full_size) {
                break;
            }

            FNET_DBG_RECV(header.payload_size);
            Span message_body = data.subspan(sizeof(Header), header.payload_size);
            auto trace_event_schedule_msg = common::Tracer::Begin("Schedule message");
            std::string response = string_pool_.Claim();
            handler({message_body.data(), message_body.size()}, response);

            Header h = MakeHeader(response.size());
            send_queue_.emplace_back(h, std::move(response), string_pool_);

            common::Tracer::End(trace_event_schedule_msg);

            data = data.subspan(full_size);
            num_bytes_processed += full_size;
        }

        buffer_.Release(num_bytes_processed);
    }

    Span GetUsedBuffer() {
        return { buffer_.GetReservedStart(), buffer_.GetNumReserved() };
    }

    void Close() {
        if (fd_ != -1) {
            FNET_THROW_IF_POSIX_ERR(close(fd_));
            fd_ = -1;
        }
        buffer_.Clear();
        send_queue_.clear();
        sent_in_message_ = 0;
    }

    void Reset(int client_fd, int epoll_fd, uint64_t client_id) {
        Close();
        fd_ = client_fd;
    }

    int Fd() const { return fd_; }

private:
    int fd_ = -1;
    RingBuffer buffer_;
    std::deque<OutgoingMessage> send_queue_;
    size_t sent_in_message_ = 0;
    ObjectPool<std::string> string_pool_;
};


struct ServerConfig {
    std::string host;
    int port = -1;
    size_t max_num_clients = 128;
    size_t client_recv_buffer_size = 16 * 1024;
    bool reuse_addr = false;
};

class Server {
    ServerConfig conf_;
    bool is_running_ = true;

public:
    Server(ServerConfig conf) : conf_(std::move(conf)) {}

    void Stop() {
        is_running_ = false;
    }

    bool IsRunning() const {
        return is_running_;
    }

    void Run(const HandleFn& handler) {
        int server_socked_fd = 0;
        FNET_THROW_IF_POSIX_ERR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));
        FNET_DEFER { FNET_THROW_IF_POSIX_ERR(close(server_socked_fd)); };
        FNET_THROW_IF_POSIX_ERR(SetFileFlag(server_socked_fd, O_NONBLOCK));

        if (conf_.reuse_addr) {
            FNET_THROW_IF_POSIX_ERR(SetSockOpt(server_socked_fd, SO_REUSEADDR, SOL_SOCKET));
        }

        sockaddr_in server_socked_addr = MakeSockaddr(conf_.host.c_str(), conf_.port);

        FNET_THROW_IF_POSIX_ERR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));
        FNET_THROW_IF_POSIX_ERR(listen(server_socked_fd, 16));

        int epoll_fd = 0;
        FNET_THROW_IF_POSIX_ERR(epoll_fd = epoll_create(1));
        FNET_DEFER { FNET_THROW_IF_POSIX_ERR(close(epoll_fd)); };

        FNET_THROW_IF_POSIX_ERR(AddEpollEvent(epoll_fd, server_socked_fd, EPOLLIN, -1));

        constexpr size_t MAX_NUM_EPOLL_EVENTS = 64;
        struct epoll_event epoll_events[MAX_NUM_EPOLL_EVENTS] = {};

        std::vector<ClientConnection> client_connections;
        std::vector<size_t> free_connections;

        auto close_client_now = [epoll_fd, &free_connections](ClientConnection& client, size_t client_id) {
            FNET_THROW_IF_POSIX_ERR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
            free_connections.push_back(client_id);
            client.Close();
        };

        while (is_running_) {
            int num_epoll_events = 0;
            {
                FNET_TRACE_SCOPE("epoll_wait");
                num_epoll_events = epoll_wait(epoll_fd, epoll_events, MAX_NUM_EPOLL_EVENTS, -1);
                if (errno == EINTR) continue;
                FNET_THROW_IF_POSIX_ERR(num_epoll_events);
            }

            for (int i = 0; i < num_epoll_events; i++) {
                struct epoll_event& ev = epoll_events[i];

                if (ev.data.u64 == uint64_t(-1)) {
                    FNET_TRACE_SCOPE("Accept");
                    int client_fd = 0;
                    FNET_THROW_IF_POSIX_ERR(client_fd = accept(server_socked_fd, nullptr, nullptr));

                    size_t client_id = -1;
                    if (free_connections.size() == 0) {
                        if (client_connections.size() == conf_.max_num_clients) {
                            FNET_THROW_IF_POSIX_ERR(close(client_fd));
                            fprintf(stderr, "Client rejected.");
                            continue;
                        }

                        client_id = client_connections.size();
                        client_connections.emplace_back(conf_.client_recv_buffer_size);
                    } else {
                        client_id = free_connections.back();
                        free_connections.pop_back();
                    }
                    FNET_ASSERT(client_id != -1);
                    
                    client_connections[client_id].Reset(client_fd, epoll_fd, client_id);
                    
                    FNET_THROW_IF_POSIX_ERR(SetSockOpt(client_fd, TCP_NODELAY, IPPROTO_TCP));
                    FNET_THROW_IF_POSIX_ERR(SetFileFlag(client_fd, O_NONBLOCK));
                    FNET_THROW_IF_POSIX_ERR(AddEpollEvent(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP, client_id));
                    continue;
                }

                size_t client_id = ev.data.u64;
                ClientConnection& client = client_connections[client_id];

                
                if (ev.events & (EPOLLHUP | EPOLLERR)) {
                    FNET_TRACE_SCOPE("EPOLLHUP | EPOLLERR");
                    close_client_now(client, client_id);
                    continue;
                }

                bool had_pending_out = client.GetSendQueueSize();
                if (ev.events & EPOLLIN) {
                    FNET_TRACE_SCOPE("EPOLLIN");
                    SockResult res = client.Recv();
                    if (res.error == SockResult::BROKEN || res.error == SockResult::DISCONNECTED) {
                        close_client_now(client, client_id);
                        continue;
                    }

                    client.ProcessIncomingTraffic(handler);
                }
                bool writes_appeared = !had_pending_out && client.GetSendQueueSize();

                if (writes_appeared || (ev.events & EPOLLOUT)) {
                    SockResult res = client.Send();
                    if (res.error == SockResult::BROKEN) {
                        close_client_now(client, client_id);
                        continue;
                    }
                }

                if ((ev.events & EPOLLRDHUP) && !client.GetSendQueueSize()) {
                    close_client_now(client, client_id);
                    continue;
                }

                FNET_TRACE_SCOPE("ModEpollEvents");
                int event_mask = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
                if (client.GetSendQueueSize()) {
                    event_mask |= EPOLLOUT;
                }

                FNET_THROW_IF_POSIX_ERR(ModEpollEvent(epoll_fd, client.Fd(), event_mask, client_id));
            }
        }
    }
};

void RunServer(ServerConfig conf, HandleFn handle) {
    Server(std::move(conf)).Run(std::move(handle));
}

class Client;

struct Request {
    struct Data {
        bool is_finished = false;
        std::string result = {};
    };

    std::shared_ptr<Data> data_;

    Request(std::string buffer) {
        data_ = std::make_shared<Data>();
        data_->result = std::move(buffer);
    }
};

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
    std::deque<Request> recv_queue_;

    ObjectPool<std::string> string_pool_;

public:
    Client(const std::string& host, int port, size_t buffer_size = 64 * 1024) : recv_buffer_(buffer_size) {
        FNET_ASSERT(buffer_size >= sizeof(Header));
        FNET_THROW_IF_POSIX_ERR(server_socket_fd_ = socket(AF_INET, SOCK_STREAM, 0));

        sockaddr_in server_sockaddr = MakeSockaddr(host.c_str(), port);

        FNET_THROW_IF_POSIX_ERR(connect(server_socket_fd_, (const struct sockaddr*) &server_sockaddr, sizeof(server_sockaddr)));
        FNET_THROW_IF_POSIX_ERR(SetSockOpt(server_socket_fd_, TCP_NODELAY, IPPROTO_TCP));
        // The socket is blocking by default.
        // Whenever we want it in the non blocking mode, we'll use MSG_DONTWAIT
    }

    ~Client() {
        Reset();
    }

    void Reset() {
        if (server_socket_fd_ != -1) {
            close(server_socket_fd_);
            server_socket_fd_ = -1;
        }

        for (Request& cr: recv_queue_) {
            // TODO: Set error
            cr.data_->is_finished = true;
        }

        send_queue_.clear();
        recv_queue_.clear();
        recv_buffer_.Clear();

        sent_in_this_iovec_ = 0;
        num_iovecs_sent_ = 0;
        recv_in_payload_ = 0;
        header_is_parsed_ = false;
    }

    Request ScheduleRequest(Span request_bytes) {
        send_queue_.push_back({MakeHeader(request_bytes.size()), request_bytes});

        Request request(string_pool_.Claim());
        recv_queue_.push_back(request);
        return request;
    }

    std::string GetResult(Request& request) {
        while (!request.data_->is_finished) {
            FNET_ASSERT(recv_queue_.size() > 0);
            DoIoCycle();
        }
        return std::move(request.data_->result);
    }

    void WaitAll() {
        while (recv_queue_.size()) {
            DoIoCycle();
        }
    }

    void ReturnRequestBuffer(std::string buffer) {
        string_pool_.Return(std::move(buffer));
    }

    std::string DoRequest(Span request_bytes) {
        FNET_ASSERT(server_socket_fd_ != -1);

        Request request = ScheduleRequest(request_bytes);
        return GetResult(request);
    }

    void DoSyncRequest(Span request, std::string& dest) {
        FNET_ASSERT(server_socket_fd_ != -1);

        WaitAll();

        SockResult send_res = SendMessage(server_socket_fd_, request);
        FNET_ASSERT(send_res);

        Header header = {};
        FNET_ASSERT(RecvFixed(server_socket_fd_, (char*) &header, sizeof(header)));
        dest.resize(header.payload_size);
        FNET_ASSERT(RecvFixed(server_socket_fd_, dest.data(), dest.size()));
    }

    void DoIoCycle() {
        bool want_send = send_queue_.size();
        bool want_recv = recv_queue_.size();

        FNET_ASSERT(want_send || want_recv);

        pollfd pfd = {};
        pfd.fd = server_socket_fd_;
        pfd.events = (POLLIN * want_recv) | (POLLOUT * want_send);
        auto trace_event = common::Tracer::Begin("poll");
        poll(&pfd, 1, -1);
        common::Tracer::End(trace_event);

        FNET_ASSERT(!(pfd.revents & POLLNVAL));

        FNET_ASSERT(!(pfd.revents & POLLERR || pfd.revents & POLLHUP));

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

                if (num_sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    break;
                }
                FNET_THROW_IF_POSIX_ERR(num_sent);

                sent_in_this_iovec_ += num_sent;
            }

            size_t num_messages_sent = num_iovecs_sent_ / 2;
            num_iovecs_sent_ -= num_messages_sent * 2;
            for (size_t i = 0; i < num_messages_sent; i++) {
                FNET_DBG_SEND(send_queue_.front().header.payload_size);
                send_queue_.pop_front();
            }
        }

        if (pfd.revents & POLLIN) {
            while (recv_queue_.size()) {
                if (header_is_parsed_) {
                    FNET_ASSERT(recv_buffer_.GetNumReserved() == 0);

                    size_t remains_in_payload = recv_queue_[0].data_->result.size() - recv_in_payload_;
                    if (remains_in_payload >= 1024) {
                        auto tev = common::Tracer::Begin("recv large");
                        int num_recv = recv(server_socket_fd_, recv_queue_[0].data_->result.data() + recv_in_payload_, remains_in_payload, MSG_NOSIGNAL | MSG_DONTWAIT);
                        common::Tracer::End(tev);

                        FNET_ASSERT(num_recv != 0);

                        if (num_recv < 0 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                            break;
                        }
                        FNET_THROW_IF_POSIX_ERR(num_recv);

                        recv_in_payload_ += num_recv;

                        if (recv_in_payload_ == recv_queue_[0].data_->result.size()) {
                            recv_queue_[0].data_->is_finished = true;
                            recv_queue_.pop_front();
                            header_is_parsed_ = false;
                            recv_in_payload_ = 0;
                        }

                        continue;
                    }
                }

                char* avail = recv_buffer_.GetFreeStart();
                size_t num_avail = recv_buffer_.GetNumFree();

                auto trace_event = common::Tracer::Begin("recv");
                int num_recv = recv(server_socket_fd_, avail, num_avail, MSG_NOSIGNAL | MSG_DONTWAIT);
                common::Tracer::End(trace_event);

                FNET_ASSERT(num_recv != 0);

                if (num_recv < 0 && (errno == EWOULDBLOCK || errno == EAGAIN)) {
                    break;
                }
                FNET_THROW_IF_POSIX_ERR(num_recv);

                recv_buffer_.Reserve(num_recv);

                // Deliver responses to Futures
                FNET_TRACE_SCOPE("Deliver responses");
                while (recv_queue_.size()) {
                    char* received = recv_buffer_.GetReservedStart();
                    size_t received_size = recv_buffer_.GetNumReserved();

                    if (!header_is_parsed_) {
                        if (received_size >= sizeof(Header)) {
                            Header& header = *((Header*) received);
                            auto trace_event_resize = common::Tracer::Begin("string::resize(%lld)", header.payload_size);
                            recv_queue_[0].data_->result.resize(header.payload_size);
                            common::Tracer::End(trace_event_resize);
                            header_is_parsed_ = true;
                            recv_buffer_.Release(sizeof(Header));
                            continue;
                        } else {
                            break;
                        }
                    }

                    size_t remains_in_payload = recv_queue_[0].data_->result.size() - recv_in_payload_;

                    if (received_size == 0 && remains_in_payload) {
                        break;
                    }

                    size_t num_to_copy = std::min(remains_in_payload, received_size);
                    auto trace_event_memcpy = common::Tracer::Begin("memcpy");
                    std::memcpy(recv_queue_[0].data_->result.data() + recv_in_payload_, received, num_to_copy);
                    common::Tracer::End(trace_event_memcpy);
                    recv_buffer_.Release(num_to_copy);
                    recv_in_payload_ += num_to_copy;

                    if (recv_in_payload_ == recv_queue_[0].data_->result.size()) {
                        recv_queue_[0].data_->is_finished = true;
                        recv_queue_.pop_front();
                        header_is_parsed_ = false;
                        recv_in_payload_ = 0;
                    }
                }
            }
        }
    }
};

} // namespace fnet

