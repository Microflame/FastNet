#include <gtest/gtest.h>

#include <sstream>

#include "fastnet.hpp"

TEST(FastNet, MessageFormat) {
    constexpr size_t msg_size = 42;
    fnet::Header h = fnet::MakeHeader(msg_size);
    EXPECT_EQ(h.header_size, sizeof(h));
    EXPECT_EQ(h.payload_size, msg_size);
    EXPECT_EQ(h.GetFullSize(), msg_size + sizeof(h));
}

TEST(FastNet, Defer) {
    int a = 0;
    {
        EXPECT_EQ(a, 0);
        FNET_DEFER { a = 42; };
        EXPECT_EQ(a, 0);
    }
    EXPECT_EQ(a, 42);
}

TEST(RingBufffer, Ctor) {
    fnet::RingBuffer rb(4096);
    EXPECT_EQ(rb.GetNumFree(), 4096);
    EXPECT_EQ(rb.GetNumTotal(), 4096);
    EXPECT_EQ(rb.GetNumReserved(), 0);
}

TEST(RingBufffer, Reserve) {
    fnet::RingBuffer rb(4096);

    rb.Reserve(4000);
    EXPECT_EQ(rb.GetNumFree(), 96);
    EXPECT_EQ(rb.GetNumTotal(), 4096);
    EXPECT_EQ(rb.GetNumReserved(), 4000);
}

TEST(RingBufffer, ReserveFull) {
    fnet::RingBuffer rb(4096);

    rb.Reserve(4096);
    EXPECT_EQ(rb.GetNumFree(), 0);
    EXPECT_EQ(rb.GetNumReserved(), 4096);
}

TEST(RingBufffer, Release) {
    fnet::RingBuffer rb(4096);

    rb.Reserve(4000);
    rb.Release(2000);
    EXPECT_EQ(rb.GetNumFree(), 2096);
    EXPECT_EQ(rb.GetNumTotal(), 4096);
    EXPECT_EQ(rb.GetNumReserved(), 2000);
}

TEST(RingBufffer, WrapAround) {
    fnet::RingBuffer rb(4096);

    char* buffer_start = rb.Reserve(4094);
    EXPECT_EQ(buffer_start[0], '\0');
    EXPECT_EQ(buffer_start[1], '\0');
    EXPECT_EQ(buffer_start[2], '\0');

    rb.Release(4094);
    char* data = rb.Reserve(16);
    EXPECT_GT(data, rb.Reserve(0));

    memcpy(data, "12345", 5);
    EXPECT_EQ(buffer_start[0], '3');
    EXPECT_EQ(buffer_start[1], '4');
    EXPECT_EQ(buffer_start[2], '5');
    EXPECT_EQ(rb.GetNumReserved(), 16);
}

class PoolTester: public fnet::PooledObject<PoolTester> {
public:
    PoolTester() = delete;

    PoolTester(size_t& counter) :
        counter_(&counter) {
        ++counter;
    }

    PoolTester(const PoolTester& other) = delete;

    PoolTester(PoolTester&& other) : counter_(other.counter_) {
        other.counter_ = nullptr;
    }

    ~PoolTester() {
        if (counter_) {
            --(*counter_);
        }
    }

    PoolTester& operator=(const PoolTester& other) = delete;

    PoolTester& operator=(PoolTester&& other) {
        counter_ = other.counter_;
        other.counter_ = nullptr;
        return *this;
    }

private:
    size_t* counter_ = nullptr;
};

TEST(ObjectPool, CreatePool) {
    size_t counter = 0;
    fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

    EXPECT_EQ(counter, 0);
}

TEST(ObjectPool, CreateObject) {
    size_t counter = 0;
    {
        fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

        {
            fnet::PooledObjectRef<PoolTester> obj = pool.GetOrCreate();
            EXPECT_EQ(counter, 1);
        }
        EXPECT_EQ(counter, 1);
    }

    EXPECT_EQ(counter, 0);
}

TEST(ObjectPool, CreateMultiple) {
    size_t counter = 0;
    {
        fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

        fnet::PooledObjectRef<PoolTester> obj0 = pool.GetOrCreate();
        fnet::PooledObjectRef<PoolTester> obj1 = pool.GetOrCreate();
        fnet::PooledObjectRef<PoolTester> obj2 = pool.GetOrCreate();

        {
            fnet::PooledObjectRef<PoolTester> obj3 = pool.GetOrCreate();
            EXPECT_EQ(counter, 4);
        }
        EXPECT_EQ(counter, 4);
    }

    EXPECT_EQ(counter, 0);
}

TEST(ObjectPool, CreateReuse) {
    size_t counter = 0;
    {
        fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

        fnet::PooledObjectRef<PoolTester> obj0 = pool.GetOrCreate();

        {
            fnet::PooledObjectRef<PoolTester> obj1 = pool.GetOrCreate();
            EXPECT_EQ(counter, 2);
        }

        {
            fnet::PooledObjectRef<PoolTester> obj1 = pool.GetOrCreate();
            EXPECT_EQ(counter, 2);
        }

        {
            fnet::PooledObjectRef<PoolTester> obj1 = pool.GetOrCreate();
            EXPECT_EQ(counter, 2);
        }

        EXPECT_EQ(counter, 2);
    }

    EXPECT_EQ(counter, 0);
}

TEST(ObjectPool, CopyRef) {
    size_t counter = 0;
    {
        fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

        fnet::PooledObjectRef<PoolTester> obj0 = pool.GetOrCreate();
        fnet::PooledObjectRef<PoolTester> obj1 = obj0;
        fnet::PooledObjectRef<PoolTester> obj2 = obj0;
        fnet::PooledObjectRef<PoolTester> obj3 = obj0;
        fnet::PooledObjectRef<PoolTester> obj4 = obj0;

        EXPECT_EQ(counter, 1);
    }

    EXPECT_EQ(counter, 0);
}

TEST(ObjectPool, ReferenceOutlivesPool) {
    size_t counter = 0;

    {
        std::vector<fnet::PooledObjectRef<PoolTester>> store;
        {
            fnet::ObjectPool<PoolTester> pool([&counter](){ return PoolTester(counter); });

            fnet::PooledObjectRef<PoolTester> obj0 = pool.GetOrCreate();
            store.emplace_back(std::move(obj0));

            EXPECT_EQ(counter, 1);
        }

        EXPECT_EQ(counter, 1);
    }
    EXPECT_EQ(counter, 0);
}
