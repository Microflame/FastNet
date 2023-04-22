#pragma once

#include <chrono>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <stdio.h>

namespace common
{

#define COMMON_ASSERT(x)                                                \
    do {                                                                \
        if (!(x)) {                                                     \
            throw std::runtime_error("(" #x ") was not fulfilled");     \
        }                                                               \
    } while (0)

using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

inline TimePoint GetTimePoint() {
    return Clock::now();
}

inline double TimeUs() {
    TimePoint now = GetTimePoint();
    double time_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    return time_us;
}

inline double TimeMs() {
    return TimeUs() / 1'000;
}

inline double TimeS() {
    return TimeUs() / 1'000'000;
}

struct TraceEvent {
    enum Type: uint8_t {
        BEGIN,
        END
    };

    uint32_t name_offset;
    uint32_t name_end;
    uint64_t time_point;
    Type type;
};

class Tracer {
public:
    static bool INITIALIZED;

    static Tracer& Get(size_t name_buffer_size = 16 * 1024, size_t events_size = 1024) {
        static Tracer tracer(name_buffer_size, events_size);
        return tracer;
    }
    
    template <typename ...Args>
    static size_t Begin(const char* name, Args... args) {
        if (!INITIALIZED) return -1;
        return Get().BeginImpl(name, args...);
    }

    static void End(size_t ev_id) {
        if (!INITIALIZED) return;
        Get().EndImpl(ev_id);
    }

    static std::string ToString() {
        if (!INITIALIZED) return {};
        return Get().ToStringImpl();
    }

private:
    std::vector<TraceEvent> events_;
    std::vector<char> name_buffer_;
    size_t name_buffer_offset_ = 0;

    Tracer(size_t name_buffer_size, size_t events_size) {
        COMMON_ASSERT(!INITIALIZED);
        INITIALIZED = true;
        events_.reserve(events_size);
        name_buffer_.resize(name_buffer_size);
    }

    template <typename ...Args>
    size_t BeginImpl(const char* name, Args... args) {
        size_t ev_id = events_.size();
        events_.push_back({});
        TraceEvent& ev = events_.back();

        constexpr size_t MAX_NAME_LEN = 1024;
        while ((name_buffer_.size() - name_buffer_offset_) < MAX_NAME_LEN) {
            name_buffer_.resize(name_buffer_.size() * 2);
        }

        ev.name_offset = name_buffer_offset_;
        int name_len = snprintf(&name_buffer_[name_buffer_offset_], MAX_NAME_LEN, name, args...);
        COMMON_ASSERT(name_len >= 0);

        name_buffer_offset_ += name_len; // TODO: Not sure about -1
        ev.name_end = name_buffer_offset_;

        ev.time_point = TimeUs();
        ev.type = TraceEvent::BEGIN;
        return ev_id;
    }

    void EndImpl(size_t ev_id) {
        events_.push_back(events_[ev_id]);
        TraceEvent& ev = events_.back();
        ev.time_point = TimeUs();
        ev.type = TraceEvent::END;
    }

    std::string ToStringImpl() const {
        const char* nbuf = name_buffer_.data();
        std::stringstream ss;
        ss << "[";
        const char* sep = "";
        for (const TraceEvent& ev: events_) {
            ss << sep;
            sep = ",";
            ss << "{\"name\": \"";
            ss << std::string_view(nbuf + ev.name_offset, nbuf + ev.name_end);
            ss << "\", \"ph\": \"" << (ev.type == TraceEvent::BEGIN ? 'B' : 'E');
            ss << "\", \"pid\": 0, \"tid\": 0, \"ts\": ";
            ss << ev.time_point;
            ss << "}";
        }
        ss << "]";
        return ss.str();
    }
};

bool Tracer::INITIALIZED = false;

} // namespace common
