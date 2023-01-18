#include "tbb_scheduler.hpp"
#include "mergesort.hpp"

#include <random>
#include <exec/static_thread_pool.hpp>
#include <stdexec/execution.hpp>

#include <concepts>
#include <iostream>
#include <new>
#include <span>
#include <ranges>

namespace ex = stdexec;
namespace stdr = std::ranges;


#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

struct Data {
  double value;
  std::uint32_t flags;
  std::uint32_t seconds;
  std::uint16_t milliseconds;
  Data() = default;
  Data(int milliseconds) : value(), flags{} , seconds(milliseconds / 1000),
                            milliseconds(milliseconds % 1000)
                            {}
  bool operator<(const Data& other) const noexcept {
    return seconds < other.seconds ||
            (seconds == other.seconds && milliseconds < other.milliseconds);
  }
};

using mn::Merge;

int main(int argc, char** argv) {
  int sequentially = 0;
  int repeat_n = 1000;
  std::size_t nThreads = 2;
  std::size_t chunkSize = 1024 * 32;
  int n = chunkSize * 100;
  int m = n;

  std::cin >> n;
  std::cin.ignore();
  std::cin >> m;
  std::cin.ignore();
  std::cin >> sequentially;
  std::cin.ignore();
  std::cin >> repeat_n;
  std::cin.ignore();
  std::cin >> nThreads;
  std::cin.ignore();
  std::cin >> chunkSize;
  std::cin.ignore();


  using T = Data;
  std::vector<T> left(n);
  std::vector<T> right(m);
  std::vector<T> result(left.size() + right.size());

  // std::random_device rd;
  std::mt19937 gen(2023);
  std::uniform_int_distribution<> distrib(1.0, INT_MAX);
  for (T& n : left) {
    n = distrib(gen);
  }
  for (T& n : right) {
    n = distrib(gen);
  }
  std::sort(left.begin(), left.end());
  std::sort(right.begin(), right.end());


  std::vector<std::chrono::nanoseconds> times;
  times.reserve(repeat_n);

  if (sequentially == 1) {
    for (int r = 0; r < repeat_n; ++r) {
      auto start = std::chrono::steady_clock::now();
      Merge(left, right, result);
      auto stop = std::chrono::steady_clock::now();
      auto nanos =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      times.push_back(nanos);
    }
  } else if (sequentially == 0) {
    // exec::static_thread_pool context(nThreads);
    auto context = nThreads > 0 ? veeam::tbb_context{nThreads} : veeam::tbb_context{};
    ex::scheduler auto scheduler = context.get_scheduler();
    for (int r = 0; r < repeat_n; ++r) {
      auto start = std::chrono::steady_clock::now();
      ex::sync_wait(Merge(scheduler, left, right, result, chunkSize));
      auto stop = std::chrono::steady_clock::now();
      auto nanos =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      times.push_back(nanos);
    }
  } else if (sequentially == 2) {
    exec::static_thread_pool context(std::max<int>(nThreads, 1));
    ex::scheduler auto scheduler = context.get_scheduler();
    for (int r = 0; r < repeat_n; ++r) {
      auto start = std::chrono::steady_clock::now();
      ex::sync_wait(Merge(scheduler, left, right, result, chunkSize));
      auto stop = std::chrono::steady_clock::now();
      auto nanos =
          std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      times.push_back(nanos);
    }
  }

  auto average = std::reduce(times.begin(), times.end()) / times.size();
  std::cout << "Average time: " << std::chrono::duration_cast<std::chrono::microseconds>(average).count() << " mu s\n";

  assert(std::is_sorted(result.begin(), result.end()));
}