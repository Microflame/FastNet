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

namespace fnet
{

// <Macros>

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

// </Macros>

// <Constants>

static const size_t PAGE_SIZE = getpagesize();

// </Constants>

// <DataStructs>

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
class PooledObject;

template <typename T>
class ObjectPool;

template <typename T>
class PooledObjectRef {
public:
    PooledObjectRef() = delete;

    PooledObjectRef(T& payload) noexcept :
        payload_(&payload) {
        IncRef();
    }

    PooledObjectRef(const PooledObjectRef& other) noexcept {
        payload_ = other.payload_;
        IncRef();
    }

    PooledObjectRef(PooledObjectRef&& other) noexcept {
        payload_ = other.payload_;
        IncRef();
    }

    ~PooledObjectRef() {
        Reset();
    }

    PooledObjectRef& operator=(const PooledObjectRef& other) {
        Reset();
        payload_ = other.payload_;
        IncRef();
    }

    PooledObjectRef& operator=(PooledObjectRef&& other) {
        Reset();
        payload_ = other.payload_;
        IncRef();
    }

    T& Get() { return *payload_; }

private:
    T* payload_ = nullptr;

    void IncRef() {
        payload_->ref_count_ += 1;
    }

    void Reset() {
        FNET_ASSERT(payload_->ref_count_ > 0);

        payload_->ref_count_ -= 1;

        if (payload_->ref_count_ == 0) {
            delete payload_;
        } else if (payload_->ref_count_ == 1 && payload_->pool_) {
            // The only reference is from pool_.all, so return to pool.free
            payload_->pool_->Return(*payload_);
        }
    }
};

template<typename T>
struct PooledObject {
    size_t ref_count_ = 0;
    ObjectPool<T>* pool_ = nullptr;
};

template <typename T>
class ObjectPool {
public:
    ObjectPool() = delete;
    ObjectPool(const ObjectPool& other) = delete;
    ObjectPool(ObjectPool&& other) = delete;
    ObjectPool& operator=(const ObjectPool& other) = delete;
    ObjectPool& operator=(ObjectPool&& other) = delete;

    ObjectPool(std::function<T()> factory) :
        factory_(std::move(factory)) {
    }

    ~ObjectPool() {
        for (PooledObjectRef<T>& obj: all_objects_) {
            obj.Get().pool_ = nullptr;
        }
    }

    PooledObjectRef<T> GetOrCreate() {
        if (free_objects_.size()) {
            PooledObjectRef<T> res = free_objects_.back();
            free_objects_.pop_back();
            return res;
        }

        T* res = new T(factory_());
        res->pool_ = this;

        all_objects_.emplace_back(*res);
        return all_objects_.back();
    }

    void Return(T& object) {
        free_objects_.emplace_back(object);
    }

private:
    std::vector<PooledObjectRef<T>> all_objects_;
    std::vector<PooledObjectRef<T>> free_objects_;
    std::function<T()> factory_;
};

// </DataStructs>

// <POSIX>

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

in_addr_t Resolve(const char* hostname) {
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

// </POSIX>

struct Header {
    uint16_t header_size;
    uint8_t header_version;
    uint64_t payload_size;

    size_t GetFullSize() const { return header_size + payload_size; }
};

static_assert(sizeof(Header) == 16);

Header MakeHeader(size_t payload_size) {
    Header header {
        .header_size = sizeof(Header),
        .header_version = 0,
        .payload_size = payload_size
    };
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

template <typename T>
class Pool {
public:
    Pool() : Pool(1) {}
    Pool(size_t initial_size) {
        FNET_ASSERT(initial_size > 0);
        pool_.resize(initial_size);
        for (size_t i = 0; i < initial_size; i++) {
            pool_[i] = std::unique_ptr<T>(new T);
        }
    }

    Pool(const Pool& other) = delete;
    Pool(Pool&& other) = delete;
    Pool& operator=(const Pool& other) = delete;
    Pool& operator=(Pool&& other) = delete;

    std::unique_ptr<T> Claim() {
        if (pool_.size() == 0) {
            size_t to_add = pool_.capacity();
            pool_.resize(to_add);
            for (size_t i = 0; i < to_add; i++) {
                pool_[i] = std::unique_ptr<T>(new T);
            }
        }

        std::unique_ptr<T> res = std::move(pool_.back());
        pool_.pop_back();
        return res;
    }

    void Return(std::unique_ptr<T> value) {
        pool_.emplace_back(std::move(value));
    }

private:
    std::vector<std::unique_ptr<T>> pool_;
};

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
    std::unique_ptr<std::string> message;
    Pool<std::string>& pool;

    OutgoingMessage(Header header, std::unique_ptr<std::string> message, Pool<std::string>& pool)
    : header(header),
      message(std::move(message)),
      pool(pool)
    {}

    ~OutgoingMessage() {
        pool.Return(std::move(message));
    }
};

using HandleFn = std::function<void(std::string_view, std::string&)>;

class ClientConnection {
public:
    ClientConnection() : buffer_(64 * 1024 * 1024) {}

    ClientConnection(const ClientConnection& other) = delete;
    ClientConnection(ClientConnection&& other) {
        fd_ = other.fd_;
        buffer_ = std::move(other.buffer_);
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
    }

    ~ClientConnection() {
        Close();
    }

    ClientConnection& operator=(const ClientConnection& other) = delete;
    ClientConnection& operator=(ClientConnection&& other) {
        Close();
        fd_ = other.fd_;
        buffer_ = std::move(other.buffer_);
        send_queue_ = std::move(other.send_queue_);
        sent_in_message_ = other.sent_in_message_;
        return *this;
    }

    bool HasPendingOutgoingMessages() const { return send_queue_.size(); }

    SockResult Recv() {
        size_t num_read_total = 0;
        while (buffer_.GetNumFree()) {
            auto trace_event = common::Tracer::Begin("recv");
            int recv_res = recv(fd_, buffer_.GetFreeStart(), buffer_.GetNumFree(), 0);
            common::Tracer::End(trace_event);

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
        if (send_queue_.size() == 0) {
            return {0, SockResult::OK};
        }

        auto trace_event = common::Tracer::Begin("Send:BuildIovecs");
        size_t num_sent_during_call = 0;
        std::vector<iovec> iovecs(2 * send_queue_.size());
        for (size_t i = 0; i < send_queue_.size(); ++i) {
            iovecs[2 * i].iov_base = &send_queue_[i].header;
            iovecs[2 * i].iov_len = sizeof(Header);
            iovecs[2 * i + 1].iov_base = send_queue_[i].message->data();
            iovecs[2 * i + 1].iov_len = send_queue_[i].message->size();
        }

        size_t num_iovecs_sent = 0;
        size_t sent_in_iovec = sent_in_message_;
        if (sent_in_iovec > sizeof(Header)) {
            sent_in_iovec -= sizeof(Header);
            num_iovecs_sent = 1;
        }
        iovecs[num_iovecs_sent].iov_base = (char*) iovecs[num_iovecs_sent].iov_base + sent_in_iovec;
        iovecs[num_iovecs_sent].iov_len -= sent_in_iovec;
        common::Tracer::End(trace_event);

        while (iovecs.size() > num_iovecs_sent) {
            msghdr msg = {
                .msg_iov = &iovecs[num_iovecs_sent],
                .msg_iovlen = iovecs.size() - num_iovecs_sent,
            };
            auto trace_event = common::Tracer::Begin("sendmsg");
            int num_sent = sendmsg(fd_, &msg, MSG_NOSIGNAL);
            common::Tracer::End(trace_event);
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
            sent_in_message_ -= send_queue_.front().header.GetFullSize();
            send_queue_.pop_front();
        }
        return {num_sent_during_call, SockResult::OK};
    }

    void ProcessIncomingTraffic(const HandleFn& handler) {
        Span data = GetUsedBuffer();
        size_t num_bytes_processed = 0;
        while (data.size() >= sizeof(Header)) {
            Header& header = *( (Header*) data.data() );
            size_t full_size = header.GetFullSize();
            if (data.size() < full_size) {
                break;
            }

            FNET_DBG_RECV(header.payload_size);
            Span message_body = data.subspan(header.header_size, header.payload_size);
            auto trace_event = common::Tracer::Begin("Schedule message");
            std::unique_ptr<std::string> response = string_pool_.Claim();
            handler({message_body.data(), message_body.size()}, *response);

            Header h = MakeHeader(response->size());
            send_queue_.emplace_back(h, std::move(response), string_pool_);

            common::Tracer::End(trace_event);

            data = data.subspan(full_size);
            num_bytes_processed += full_size;
        }

        buffer_.Release(num_bytes_processed);
    }

    Span GetUsedBuffer() {
        return {buffer_.GetReservedStart(), buffer_.GetNumReserved()};
    }

    void Close() {
        if (fd_ != -1) {
            FNET_THROW_IF_POSIX_ERR( close(fd_) );
            fd_ = -1;
        }
        buffer_.Clear();
        send_queue_.clear();
        sent_in_message_ = 0;
    }

    void Reset(int fd) {
        Close();
        fd_ = fd;
    }

    int Fd() const { return fd_; }

private:
    int fd_ = -1;
    RingBuffer buffer_;
    std::deque<OutgoingMessage> send_queue_;
    size_t sent_in_message_ = 0;
    Pool<std::string> string_pool_;
};


struct ServerConfig {
    std::string host;
    int port = -1;
    size_t max_num_clients = 128;
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

        sockaddr_in server_socked_addr = {};
        server_socked_addr.sin_family = AF_INET;
        server_socked_addr.sin_port = htons(conf_.port);
        server_socked_addr.sin_addr.s_addr = Resolve(conf_.host.c_str());

        FNET_THROW_IF_POSIX_ERR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

        FNET_THROW_IF_POSIX_ERR(listen(server_socked_fd, 16));

        int epoll_fd = 0;
        FNET_THROW_IF_POSIX_ERR(epoll_fd = epoll_create(1));
        FNET_DEFER { FNET_THROW_IF_POSIX_ERR(close(epoll_fd)); };

        FNET_THROW_IF_POSIX_ERR(AddEpollEvent(epoll_fd, server_socked_fd, EPOLLIN, -1));

        constexpr size_t MAX_NUM_EPOLL_EVENTS = 64;
        struct epoll_event epoll_events[MAX_NUM_EPOLL_EVENTS] = {};

        std::vector<ClientConnection> clients(conf_.max_num_clients);
        IndexPool client_index_pool(conf_.max_num_clients);
        while (is_running_) {
            auto trace_event = common::Tracer::Begin("epoll_wait");
            int epoll_wait_res = epoll_wait(epoll_fd, epoll_events, MAX_NUM_EPOLL_EVENTS, -1);
            common::Tracer::End(trace_event);
            if (errno == EINTR) continue;
            FNET_THROW_IF_POSIX_ERR(epoll_wait_res);

            for (int i = 0; i < epoll_wait_res; i++) {
                struct epoll_event& ev = epoll_events[i];

                auto trace_event = common::Tracer::Begin("Accept");
                if (ev.data.u64 == uint64_t(-1)) {
                    int client_fd = 0;
                    FNET_THROW_IF_POSIX_ERR(client_fd = accept(server_socked_fd, nullptr, nullptr));
                    if (!client_index_pool.NumAvailable()) {
                        FNET_THROW_IF_POSIX_ERR(close(client_fd));
                        fprintf(stderr, "Client rejected.");
                        common::Tracer::End(trace_event);
                        continue;
                    }

                    FNET_THROW_IF_POSIX_ERR(SetSockOpt(client_fd, TCP_NODELAY, IPPROTO_TCP));
                    FNET_THROW_IF_POSIX_ERR(SetFileFlag(client_fd, O_NONBLOCK));
                    size_t client_id = client_index_pool.Claim();
                    clients[client_id].Reset(client_fd);
                    FNET_THROW_IF_POSIX_ERR(AddEpollEvent(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP, client_id));
                    common::Tracer::End(trace_event);
                    continue;
                }
                common::Tracer::End(trace_event);

                size_t client_id = ev.data.u64;
                ClientConnection& client = clients[client_id];

                trace_event = common::Tracer::Begin("EPOLLHUP | EPOLLRDHUP | EPOLLERR");
                if (ev.events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                    FNET_THROW_IF_POSIX_ERR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                    client_index_pool.Return(client_id);
                    client.Close();
                    common::Tracer::End(trace_event);
                    continue;
                }
                common::Tracer::End(trace_event);

                trace_event = common::Tracer::Begin("EPOLLIN");
                bool had_pending_out = client.HasPendingOutgoingMessages();
                if (ev.events & EPOLLIN) {
                    auto trace_client_recv = common::Tracer::Begin("client.Recv()");
                    SockResult res = client.Recv();
                    common::Tracer::End(trace_client_recv);
                    if (res.error == SockResult::BROKEN || res.error == SockResult::DISCONNECTED) {
                        FNET_THROW_IF_POSIX_ERR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                        client_index_pool.Return(client_id);
                        client.Close();
                        common::Tracer::End(trace_event);
                        continue;
                    }

                    auto trace_ProcessIncomingTraffic = common::Tracer::Begin("ProcessIncomingTraffic");
                    client.ProcessIncomingTraffic(handler);
                    common::Tracer::End(trace_ProcessIncomingTraffic);
                }
                bool writes_appeared = !had_pending_out && client.HasPendingOutgoingMessages();
                common::Tracer::End(trace_event);

                trace_event = common::Tracer::Begin("EPOLLOUT");
                if (writes_appeared || (ev.events & EPOLLOUT)) {
                    SockResult res = client.Send();
                    if (res.error == SockResult::BROKEN) {
                        FNET_THROW_IF_POSIX_ERR(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr));
                        client_index_pool.Return(client_id);
                        client.Close();
                        common::Tracer::End(trace_event);
                        continue;
                    }
                }
                common::Tracer::End(trace_event);

                trace_event = common::Tracer::Begin("ModEpollEvents");
                int event_mask = EPOLLIN | EPOLLRDHUP | EPOLLHUP;
                if (client.HasPendingOutgoingMessages()) {
                    event_mask |= EPOLLOUT;
                }

                FNET_THROW_IF_POSIX_ERR(ModEpollEvent(epoll_fd, client.Fd(), event_mask, client_id));
                common::Tracer::End(trace_event);
            }
        }
    }
};

void RunServer(ServerConfig conf, HandleFn handle) {
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
        FNET_DBG_RECV(response.size());
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

public:
    Client(const std::string& host, int port, size_t buffer_size = 64 * 1024) : recv_buffer_(buffer_size) {
        FNET_ASSERT(buffer_size >= sizeof(Header));
        FNET_THROW_IF_POSIX_ERR(server_socket_fd_ = socket(AF_INET, SOCK_STREAM, 0));

        sockaddr_in server_sockaddr = {};
        server_sockaddr.sin_family = AF_INET;
        server_sockaddr.sin_port = htons(port);
        server_sockaddr.sin_addr.s_addr = Resolve(host.c_str());

        FNET_THROW_IF_POSIX_ERR(connect(server_socket_fd_, (const struct sockaddr*) &server_sockaddr, sizeof(server_sockaddr)));
        FNET_THROW_IF_POSIX_ERR(SetSockOpt(server_socket_fd_, TCP_NODELAY, IPPROTO_TCP));
        // The socket is blocking by default.
        // Whenever we want it in the non blocking mode, we'll use MSG_DONTWAIT
    }

    ~Client() {
        Stop();
    }

    void ScheduleSend(Span request) {
        send_queue_.push_back({MakeHeader(request.size()), request});
    }

    RequestFuturePtr ScheduleRecv(std::string dest) {
        RequestFuturePtr future = std::make_unique<RequestFuture>(this);
        future->response = std::move(dest);
        recv_queue_.push_back(future.get());
        return future;
    }

    RequestFuturePtr ScheduleRequest(Span request, std::string dest = {}) {
        ScheduleSend(request);
        return ScheduleRecv(std::move(dest));
    }

    void Wait() {
        while (recv_queue_.size()) {
            DoIoCycle();
        }
    }

    std::string DoRequest(Span request, std::string dest = {}) {
        FNET_ASSERT(server_socket_fd_ != -1);

        auto rf = ScheduleRequest(request, std::move(dest));
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

        // TODO: this may cause use-after-free if user has already
        // discarded this request. Use intrusive ptr to count refs?
        // for (RequestFuture* rf: recv_queue_) {
        //     rf->SetError();
        // }

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
                        FNET_THROW_IF_POSIX_ERR(num_recv);

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

                char* avail = recv_buffer_.GetFreeStart();
                size_t num_avail = recv_buffer_.GetNumFree();

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
                FNET_THROW_IF_POSIX_ERR(num_recv);

                recv_buffer_.Reserve(num_recv);

                // Deliver responses to Futures
                trace_event = common::Tracer::Begin("Deliver responses");
                while (recv_queue_.size()) {
                    char* reserved = recv_buffer_.GetReservedStart();
                    size_t reserved_size = recv_buffer_.GetNumReserved();

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

