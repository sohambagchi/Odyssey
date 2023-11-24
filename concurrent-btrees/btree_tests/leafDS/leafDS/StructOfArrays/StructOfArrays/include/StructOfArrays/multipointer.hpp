#pragma once

#include <array>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>
#include <sys/ucontext.h>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

template <typename... Ts> class MultiPointer {
public:
  using T = std::tuple<Ts...>;
  using pointer_type = std::tuple<Ts *...>;

  struct reference_type {

    std::tuple<Ts &...> data;
    reference_type(std::tuple<Ts &...> _data) : data{_data} {}
    operator std::tuple<Ts &...>() { return data; }
    operator std::tuple<Ts...>() { return data; }
    reference_type &operator=(T other) {
      data = other;
      return *this;
    }

    auto operator<=>(const reference_type &rhs) const {
      return data <=> rhs.data;
    }

    auto operator<=>(T rhs) const { return data <=> rhs; }
    auto operator==(T rhs) const { return data == rhs; }
  };

  using difference_type = uint64_t;
  using value_type = T;
  using pointer = pointer_type;
  using reference = reference_type;
  using iterator_category = std::random_access_iterator_tag;

  static constexpr std::size_t num_types = sizeof...(Ts);
  template <int I> using NthType = typename std::tuple_element<I, T>::type;

private:
  pointer_type pointers;

  template <size_t... Is>
  std::tuple<Ts &...>
  get_impl(size_t i,
           [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) {
    return std::forward_as_tuple(std::get<Is>(pointers)[i]...);
  }

  template <size_t... Is>
  std::tuple<const Ts &...> get_impl(
      size_t i,
      [[maybe_unused]] std::integer_sequence<size_t, Is...> int_seq) const {
    return std::forward_as_tuple(std::get<Is>(pointers)[i]...);
  }

  template <typename Function, size_t... Is>
  static auto transform_impl(pointer_type const &inputs, Function function,
                             std::index_sequence<Is...>) {
    return pointer_type{function(std::get<Is>(inputs))...};
  }

  template <typename Function>
  static auto transform(pointer_type const &inputs, Function function) {
    return transform_impl(inputs, function,
                          std::make_index_sequence<sizeof...(Ts)>{});
  }

  template <typename Function, size_t... Is>
  static void apply_impl(pointer_type &inputs, Function function,
                         std::index_sequence<Is...>) {
    (function(std::get<Is>(inputs)), ...);
  }

  template <typename Function>
  static void apply(pointer_type &inputs, Function function) {
    apply_impl(inputs, function, std::make_index_sequence<sizeof...(Ts)>{});
  }

  template <typename T1, typename... rest>
  static std::tuple<rest...>
  leftshift_tuple(const std::tuple<T1, rest...> &tuple) {
    return std::apply([](auto &&, auto &...args) { return std::tie(args...); },
                      tuple);
  }

  template <std::size_t... I>
  static std::tuple<Ts *...> MakeTuplePtr(std::tuple<Ts &...> &tuple,
                                          std::index_sequence<I...>) {
    return std::make_tuple(&std::get<I>(tuple)...);
  }

  static std::tuple<Ts *...> MakeTuplePtr(std::tuple<Ts &...> &tuple) {
    return MakeTuplePtr(tuple, std::make_index_sequence<sizeof...(Ts)>{});
  }

  template <std::size_t... I>
  static std::tuple<Ts &...> MakeTupleRef(std::tuple<Ts...> &tuple,
                                          std::index_sequence<I...>) {
    return std::forward_as_tuple(std::get<I>(tuple)...);
  }

  static std::tuple<Ts &...> MakeTupleRef(std::tuple<Ts...> &tuple) {
    return MakeTupleRef(tuple, std::make_index_sequence<sizeof...(Ts)>{});
  }

  template <typename... T>
  static void set_and_zero_impl(std::tuple<T *...> location,
                                std::tuple<T &...> data) {
    std::memcpy(std::get<0>(location), &std::get<0>(data),
                sizeof(std::get<0>(data)));
    std::memset(&std::get<0>(data), 0, sizeof(std::get<0>(data)));
    if constexpr (sizeof...(T) > 1) {
      set_and_zero_impl(leftshift_tuple(location), leftshift_tuple(data));
    }
  }

  template <typename... T>
  static void set_impl(std::tuple<T *...> location,
                       std::tuple<const T &...> data) {
    std::memcpy(std::get<0>(location), &std::get<0>(data),
                sizeof(std::get<0>(data)));
    if constexpr (sizeof...(T) > 1) {
      set_impl(leftshift_tuple(location), leftshift_tuple(data));
    }
  }

public:
  MultiPointer(Ts *...ps) : pointers(std::make_tuple(ps...)) {}
  MultiPointer(pointer_type ps) : pointers(ps) {}

  MultiPointer(std::tuple<Ts &...> ts) : pointers(MakeTuplePtr(ts)) {}

  template <size_t I = 0> auto get_pointer() { return std::get<I>(pointers); }
  template <size_t I = 0> auto get_pointer() const {
    return std::get<I>(pointers);
  }

  template <size_t I, size_t... Is> auto get(size_t i) {
    if constexpr (sizeof...(Is) == 0) {
      return std::get<I>(pointers)[i];
    } else {
      return get_impl<I, Is...>(i, {});
    }
  }

  template <size_t I = 0, size_t... Is> auto get(int64_t i = 0) const {
    if constexpr (sizeof...(Is) == 0) {
      return std::get<I>(pointers)[i];
    } else {
      return get_impl<I, Is...>(i, {});
    }
  }

  std::tuple<Ts &...> operator[](std::size_t idx) {
    return get_impl(idx, std::make_index_sequence<num_types>{});
  }
  std::tuple<const Ts &...> operator[](std::size_t idx) const {
    return get_impl(idx, std::make_index_sequence<num_types>{});
  }

  MultiPointer get_raw_bytes(size_t byte_offset) const {
    pointer_type new_pointers = transform(pointers, [byte_offset](auto *data) {
      uint8_t *byte_ptr = (uint8_t *)data;
      void *pointer = nullptr;
      // this assumes that all pointers have the same size
      memcpy(&pointer, (void *)(byte_ptr + byte_offset), sizeof(void *));
      return (decltype(data))pointer;
    });
    return std::make_from_tuple<MultiPointer>(new_pointers);
  }

  reference_type operator*() { return operator[](0); }
  std::tuple<const Ts &...> operator*() const { return operator[](0); }

  MultiPointer operator+(uint64_t i) {

    pointer_type new_pointers =
        transform(pointers, [i](auto *data) { return data + i; });
    return std::make_from_tuple<MultiPointer>(new_pointers);
  }

  MultiPointer operator-(uint64_t i) const {

    pointer_type new_pointers =
        transform(pointers, [i](auto *data) { return data - i; });
    return std::make_from_tuple<MultiPointer>(new_pointers);
  }

  size_t operator-(const MultiPointer &rhs) {
    return get_pointer() - rhs.get_pointer();
  }

  MultiPointer &operator++() {
    apply(pointers, [](auto &p) { ++p; });
    return *this;
  }
  MultiPointer &operator+=(size_t i) {
    apply(pointers, [&i](auto &p) { p += i; });
    return *this;
  }
  MultiPointer &operator--() {
    apply(pointers, [](auto &p) { --p; });
    return *this;
  }

  auto operator<=>(const MultiPointer &rhs) const {

    return get_pointer() <=> rhs.get_pointer();
  }
  auto operator!=(const MultiPointer &rhs) const {

    return get_pointer() != rhs.get_pointer();
  }

  auto operator==(const MultiPointer &rhs) const {

    return get_pointer() == rhs.get_pointer();
  }
  void free_all() {
    apply(pointers, [](auto *p) { free(p); });
  }

  void free_first() { free(std::get<0>(pointers)); }

  auto left_shift() { return leftshift_tuple(pointers); }

  friend void swap(reference_type l, reference_type r) {
    T temp = l;
    l = r;
    r = temp;
  }

  void zero() {
    apply(pointers, [](auto *p) { std::memset(p, 0, sizeof(*p)); });
  }

  void set_and_zero(std::tuple<Ts &...> ts) { set_and_zero_impl(pointers, ts); }
  void set_and_zero(std::tuple<Ts...> &ts) { set_and_zero(MakeTupleRef(ts)); }
  void set(std::tuple<const Ts &...> ts) { set_impl(pointers, ts); }
  void deconstruct() {
    apply(pointers, [](auto *p) { std::destroy_at(p); });
  }
};
