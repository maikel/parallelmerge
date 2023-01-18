#ifndef MN_MERGESORT_HPP
#define MN_MERGESORT_HPP

#include <stdexec/execution.hpp>

#include <concepts>
#include <ranges>
#include <span>

namespace mn {

namespace ex = stdexec;
namespace stdr = std::ranges;

template <typename T, typename S>
concept LessThanComparable = requires(T a, S b) {
                               { a < b } -> std::convertible_to<bool>;
                             };

template <stdr::input_range L, stdr::input_range R, typename O>
requires LessThanComparable<stdr::range_value_t<R>, stdr::range_value_t<L>> &&
         stdr::output_range<O, std::ranges::range_value_t<L>> &&
         stdr::output_range<O, std::ranges::range_value_t<R>>
void Merge(L&& lhs, R&& rhs, O&& output) {
  using T = std::ranges::range_value_t<L>;
  std::input_iterator auto left = lhs.begin();
  std::input_iterator auto right = rhs.begin();
  std::output_iterator<T> auto out = output.begin();
  while (left != lhs.end() && right != rhs.end()) {
    if (!(*right < *left)) {
      *out++ = *left++;
    } else {
      *out++ = *right++;
    }
  }
  std::copy(left, lhs.end(), out);
  std::copy(right, rhs.end(), out);
}

template <stdr::input_range Needles, stdr::random_access_range Haystack,
          stdr::random_access_range Output>
requires std::is_assignable_v<stdr::range_reference_t<Output>,
                              stdr::range_reference_t<Needles>>
void Ranking(Needles&& needles, Haystack&& haystack, Output&& output,
             auto binary_search_bound) {
  std::random_access_iterator auto hint = haystack.begin();
  std::input_iterator auto needle = needles.begin();
  for (std::ptrdiff_t ni = 0; needle != needles.end(); ++ni) {
    hint = binary_search_bound(hint, haystack.end(), *needle);
    const std::ptrdiff_t hi = std::distance(haystack.begin(), hint);
    output[ni + hi] = *needle++;
  }
}

template <stdr::input_range Needles, stdr::random_access_range Haystack,
          stdr::random_access_range Output>
requires LessThanComparable<stdr::range_value_t<Haystack>,
                            stdr::range_value_t<Needles>> &&
         std::is_assignable_v<stdr::range_reference_t<Output>,
                              stdr::range_reference_t<Needles>>
void LowerRanking(Needles&& needles, Haystack&& haystack, Output&& output) {
  return Ranking(needles, haystack, output,
                 [](auto first, auto last, const auto& value) {
                   return std::lower_bound(first, last, value);
                 });
}

template <stdr::input_range Needles, stdr::random_access_range Haystack,
          stdr::random_access_range Output>
requires LessThanComparable<stdr::range_value_t<Needles>,
                            stdr::range_value_t<Haystack>> &&
         std::is_assignable_v<stdr::range_reference_t<Output>,
                              stdr::range_reference_t<Needles>>
void UpperRanking(Needles&& needles, Haystack&& haystack, Output&& output) {
  return Ranking(needles, haystack, output,
                 [](auto first, auto last, const auto& value) {
                   return std::upper_bound(first, last, value);
                 });
}

template <ex::scheduler Scheduler, stdr::contiguous_range L,
          stdr::contiguous_range R, stdr::contiguous_range O, std::integral I>
auto Merge(Scheduler scheduler, L&& lhs, R&& rhs, O&& output, I chunkSize)
    -> ex::sender auto{
  auto left = std::span{lhs};
  auto right = std::span{rhs};
  auto out = std::span{output};
  std::integral auto nLeft = lhs.size() / chunkSize + (lhs.size() % chunkSize != 0);
  ex::sender auto leftRanking =
      ex::schedule(scheduler) | ex::bulk(nLeft, [=](std::size_t i) noexcept {
        std::integral auto offset = i * chunkSize;
        stdr::random_access_range auto needles =
            stdr::views::drop(left, offset) | stdr::views::take(chunkSize);
        LowerRanking(needles, right, stdr::views::drop(out, offset));
      });
  std::integral auto nRight = rhs.size() / chunkSize + (rhs.size() % chunkSize != 0);
  ex::sender auto rightRanking =
      ex::schedule(scheduler) | ex::bulk(nRight, [=](std::size_t i) noexcept {
        std::integral auto offset = i * chunkSize;
        stdr::random_access_range auto needles =
            stdr::views::drop(right, offset) | stdr::views::take(chunkSize);
        UpperRanking(needles, left, stdr::views::drop(out, offset));
      });
  return ex::when_all(leftRanking, rightRanking);
}

} // namespace mn

#endif