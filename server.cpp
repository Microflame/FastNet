#include <iostream>
#include <vector>

#include <sys/epoll.h>
#include <fcntl.h>

#include "common.hpp"

constexpr size_t MAX_NUM_CLIENTS = 128;
constexpr size_t CLIENT_BUFFER_SIZE = 2 * 1024 * 1024;

template <typename F> 
struct DeferImpl {
    DeferImpl(F f): fn(f) {}
    ~DeferImpl() { fn(); }
    F fn;
};

#define FNET_CONCAT_IMPL(a, b) a ## b
#define FNET_CONCAT(a, b) FNET_CONCAT_IMPL(a, b)
#define FNET_DEFER DeferImpl FNET_CONCAT(defer_, __LINE__) = [&]()

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

class IndexPool {
public:
    IndexPool(size_t size) : pool_(size), num_available_(size) {
        for (size_t i = 0; i < size; i++) {
            pool_[i] = i;
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

class ClientPipe {
public:
    ClientPipe(int fd, size_t buffer_size) :
    fd_(fd),
    buffer_(buffer_size) {}

    void Close() {
        if (fd_ != -1) {
            FNET_EXIT_IF_ERROR( close(fd_) );
            fd_ = -1;
        }
    }

    void Reset(int fd) {
        Close();
        fd_ = fd;
    }

    int Fd() const { return fd_; }

private:
    int fd_ = -1;
    std::vector<char> buffer_;
};

int main(int argc, char* argv[]) {
    InitProcess();
    FNET_ASSERT(argc == 2);
    int port = std::atoll(argv[1]);

    int server_socked_fd = 0;
    FNET_EXIT_IF_ERROR(server_socked_fd = socket(AF_INET, SOCK_STREAM, 0));
    FNET_DEFER { FNET_EXIT_IF_ERROR(close(server_socked_fd)); };
    FNET_EXIT_IF_ERROR(SetFileFlag(server_socked_fd, O_NONBLOCK));
    int enable = 1;
    FNET_EXIT_IF_ERROR(setsockopt(server_socked_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)));

    sockaddr_in server_socked_addr = {};
    server_socked_addr.sin_family = AF_INET;
    server_socked_addr.sin_port = htons(port);
    server_socked_addr.sin_addr.s_addr = MakeIpAddr(127, 0, 0, 1);

    FNET_EXIT_IF_ERROR(bind(server_socked_fd, (const struct sockaddr*) &server_socked_addr, sizeof(server_socked_addr)));

    FNET_EXIT_IF_ERROR(listen(server_socked_fd, 16));

    int epoll_fd = 0;
    FNET_EXIT_IF_ERROR(epoll_fd = epoll_create(1));
    FNET_DEFER { FNET_EXIT_IF_ERROR(close(epoll_fd)); };
    FNET_EXIT_IF_ERROR(AddEpollEvent(epoll_fd, server_socked_fd, EPOLLIN, -1));

    std::vector<char> buffer(64 * 1024 * 1024);
    constexpr size_t MAX_NUM_EPOLL_EVENTS = 64;
    struct epoll_event epoll_events[MAX_NUM_EPOLL_EVENTS] = {};

    std::vector<ClientPipe> clients(MAX_NUM_CLIENTS);
    IndexPool client_index_pool(MAX_NUM_CLIENTS);
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
                    std::cerr << "Client rejected.\n";
                    continue;
                }
                std::cerr << "Client connected.\n";

                SetFileFlag(client_fd, O_NONBLOCK);
                size_t client_id = client_index_pool.Claim();
                clients[client_id].Reset(client_fd);
                AddEpollEvent(epoll_fd, client_fd, EPOLLIN | EPOLLRDHUP | EPOLLHUP, client_id);
                continue;
            }

            size_t client_id = ev.data.u64;
            ClientPipe& client = clients[client_id];

            if (ev.events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client.Fd(), nullptr);
                client.Close();
                std::cerr << "Client disconnected.\n";
            }

            if (ev.events & EPOLLIN) {
                
            }
        }
        



        while (true) {
            SockResult res = RecvMessage_single(client_fd, {buffer.data(), buffer.size()});
            if (!res) break;
            std::string response = "OK!\0";
            res = SendMessage_sendmsg(client_fd, {response.data(), response.size()});
            if (!res) break;
        }
    }

    return 0;
}