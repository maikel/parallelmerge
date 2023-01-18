#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <iostream>
#include <new>
#include <string>
#include <vector>

#include "mergesort.hpp"

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

int main(int argc, char** argv) {
  std::vector<int> left = {1, 2, 2, 3, 6, 7, 9, 11, 13, 15, 16, 17, 19};
  std::vector<int> right = {2, 4, 6, 8, 10, 12, 13, 13, 14, 16};

  std::size_t chunkSize = hardware_destructive_interference_size / sizeof(int);
  std::size_t nThreads = 1;
  if (argc > 1) {
    std::size_t pos = 0;
    nThreads = std::stoi(argv[1], &pos);
  }
  if (argc > 2) {
    std::size_t pos = 0;
    chunkSize = std::stoi(argv[2], &pos);
  }

  std::vector<int> result_parallel(left.size() + right.size());
  exec::static_thread_pool context(nThreads);
  stdexec::scheduler auto scheduler = context.get_scheduler();
  stdexec::sync_wait(mn::Merge(scheduler, left, right, result_parallel, chunkSize));

  std::vector<int> result_sequential(left.size() + right.size());
  mn::Merge(left, right, result_sequential);

  assert(std::is_sorted(result_parallel.begin(), result_parallel.end()));
  assert(result_parallel == result_sequential);
  for (int x : result_parallel) {
    std::cout << x << '\n';
  }
}