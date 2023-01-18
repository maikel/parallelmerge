cmake_minimum_required(VERSION 3.16)

project(mergeSort CXX)

find_package(stdexec REQUIRED)
find_package(Threads REQUIRED)
find_package(TBB)

add_executable(example example.cpp)
target_link_libraries(example STDEXEC::stdexec)
target_include_directories(example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(example PUBLIC cxx_std_20)

if (TARGET TBB::tbb)
  add_executable(benchmark benchmark.cpp)
  target_link_libraries(benchmark STDEXEC::stdexec TBB::tbb)
  target_include_directories(benchmark PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_compile_features(benchmark PUBLIC cxx_std_20)
endif()