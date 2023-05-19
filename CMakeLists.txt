cmake_minimum_required(VERSION 3.0)

project(FastNet)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -g -Wall -pedantic -Wno-unused-function -Wno-format-security")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -static-libasan -fno-omit-frame-pointer -g -Wall -pedantic -Wno-unused-function -Wno-format-security")

include_directories(.)

add_executable(server utils/server.cpp)
add_executable(client utils/client.cpp)
add_executable(client_perf_sync utils/client_perf_sync.cpp)
add_executable(client_perf_async utils/client_perf_async.cpp)
add_executable(recv_perf experiments/recv_perf.cpp)

add_executable(integ_test_async test/integ_test_async.cpp)
add_executable(fastnet_test test/fastnet_test.cpp)


include(FetchContent)
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()


FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.13.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
enable_testing()
target_link_libraries(fastnet_test GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(fastnet_test)
