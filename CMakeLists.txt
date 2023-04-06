cmake_minimum_required(VERSION 3.0)

project(FastNet)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -fno-omit-frame-pointer -g -Wall -pedantic -Wno-unused-function")

add_executable(server server.cpp)
add_executable(client client.cpp)