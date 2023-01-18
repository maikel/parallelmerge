#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <iostream>
#include <new>
#include <string>
#include <vector>

#include "tbb_scheduler.hpp"
#include "mergesort.hpp"

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

using mn::Merge;

int main(int argc, char** argv) {
  std::vector<int> left = {1, 2, 2, 3, 6, 7, 9, 11, 13, 15, 16, 17, 19};
  std::vector<int> right = {2, 4, 6, 8, 10, 12, 13, 13, 14, 16};
  std::vector<int> result(left.size() + right.size());

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
  exec::static_thread_pool context(nThreads);
  stdexec::scheduler auto scheduler = context.get_scheduler();
  stdexec::sender auto merge = Merge(scheduler, left, right, result, chunkSize);
  stdexec::sync_wait(merge);

  assert(std::is_sorted(result.begin(), result.end()));
  for (int x : result) {
    std::cout << x << '\n';
  }
}