#include "gdd_segment.hpp"

#include <algorithm>

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename T>
GddSegment<T>::GddSegment(const std::shared_ptr<const pmr_vector<T>>& values,
                          const std::shared_ptr<const pmr_vector<bool>>& null_values)
    : AbstractEncodedSegment(data_type_from_type<T>()),
      _values{values},
      _null_values{null_values} 
    { }

template <typename T>
std::shared_ptr<const pmr_vector<T>> GddSegment<T>::values() const {
  return _values;
}

template <typename T>
std::shared_ptr<const pmr_vector<bool>> GddSegment<T>::null_values() const {
  return _null_values;
}


template <typename T>
AllTypeVariant GddSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
ChunkOffset GddSegment<T>::size() const {
  return values.size();
}

template <typename T>
std::shared_ptr<AbstractSegment> GddSegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_values = std::make_shared<pmr_vector<T>>(*_values, alloc);
  auto new_null_values = std::make_shared<pmr_vector<bool>>(*_null_values, alloc);

  auto copy = std::make_shared<GddSegment<T>>(new_values, new_null_values);
  copy->access_counter = access_counter;

  return copy;
}

template <typename T>
size_t GddSegment<T>::memory_usage(const MemoryUsageCalculationMode mode) const {
  return values.size() * sizeof(typename decltype(_values)::element_type::value_type) + null_values.size();
}

template <typename T>
EncodingType GddSegment<T>::encoding_type() const {
  return EncodingType::GDD;
}

template <typename T>
std::optional<CompressedVectorType> GddSegment<T>::compressed_vector_type() const {
  return std::nullopt;
}

//EXPLICITLY_INSTANTIATE_DATA_TYPES(GddSegment);
template class GddSegment<int64_t>;
template class GddSegment<int32_t>;

}  // namespace opossum
